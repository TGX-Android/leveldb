
/*
 * This file is a part of LevelDB Preferences
 * Copyright © Vyacheslav Krylov (slavone@protonmail.ch) 2014-2021
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * File created on 04/19/2018
 */

package me.vkryl.leveldb;

import android.content.SharedPreferences;
import android.os.Build;
import android.os.ConditionVariable;
import android.os.SystemClock;
import android.util.Log;

import androidx.annotation.Keep;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({"ConstantConditions", "NullableProblems"/*, "unused"*/})
public final class LevelDB implements SharedPreferences, SharedPreferences.Editor {
  private static final String LOG_TAG = "LevelDB";

  public interface ErrorHandler {
    boolean onFatalError (LevelDB levelDB, Throwable error);
    void onError (LevelDB levelDB, String message, @Nullable Throwable error);
  }

  private final String path;
  private long ptr;
  private final long batchPtr;

  private volatile boolean isClosed;
  private boolean isEditing;
  private final AtomicBoolean repairAttempted = new AtomicBoolean(false);

  private final Object editLock;
  private final Semaphore semaphore;

  private final ConditionVariable reopenLock;

  private ErrorHandler errorHandler;

  public LevelDB (String path, boolean isThreadSafe, @Nullable ErrorHandler errorHandler) {
    this.path = path;
    this.errorHandler = errorHandler;
    openDatabase();
    batchPtr = NativeBridge.dbBatchCreate();
    if (batchPtr == 0)
      throw new AssertionError("batchPtr == 0");
    editLock = new Object();
    reopenLock = new ConditionVariable(true);
    semaphore = isThreadSafe ? new Semaphore(1) : null;
  }

  public void setErrorHandler (ErrorHandler handler) {
    this.errorHandler = handler;
  }

  @Keep
  void onFatalError (String message) {
    onFatalError(new RuntimeException(message));
  }

  private void onFatalError (Throwable error) {
    if (errorHandler != null && errorHandler.onFatalError(this, error)) {
      return;
    }
    throw error instanceof RuntimeException ? (RuntimeException) error : new RuntimeException(error);
  }

  private void onError (String message, @Nullable Throwable error) {
    if (errorHandler != null) {
      errorHandler.onError(this, message, error);
    } else {
      Log.e(LOG_TAG, message, error);
    }
  }

  private static long getTotalSize (File file) {
    if (!file.exists()) {
      return 0;
    } else if (file.isDirectory()) {
      long totalSize = 0;
      File[] files = file.listFiles();
      if (files != null) {
        for (File nested : files) {
          totalSize += getTotalSize(nested);
        }
      }
      return totalSize;
    } else {
      return file.length();
    }
  }

  public long length () {
    flush();
    return getTotalSize(new File(path));
  }

  private void openDatabase () {
    if (isClosed)
      throw new IllegalStateException();
    if (ptr != 0)
      throw new IllegalStateException();
    ptr = NativeBridge.dbOpen(this, path);
    if (ptr == 0)
      throw new AssertionError("ptr == 0");
  }

  private void closeIterators () {
    for (int i = openIterators.size() - 1; i >= 0; i--) {
      openIterators.get(i).release();
    }
  }

  public void flush () {
    if (isClosed)
      throw new IllegalStateException();
    if (BuildConfig.DEBUG) {
      // FIXME: find out the exact reason google/leveldb assertion fails:
      // leveldb/db/version_set.cc:755: leveldb::VersionSet::~VersionSet(): assertion "dummy_versions_.next_ == &dummy_versions_" failed'
      return;
    }
    reopenLock.close();
    closeIterators();
    NativeBridge.dbClose(ptr); ptr = 0;
    openDatabase();
    reopenLock.open();
  }

  public boolean repair (Throwable error) {
    if (error == null || error instanceof FileNotFoundException || !(error instanceof AssertionError))
      throw new IllegalArgumentException(error);
    String message = error.getMessage();
    if (message == null || message.isEmpty())
      return false;
    if (!message.contains("Corruption: not an sstable (bad magic number)") && // dbGet:501: Corruption: not an sstable (bad magic number)
        !message.contains(".ldb: No such file or directory")) // dbPut:637: NotFound: /data/user/0/org.thunderdog.challegram/files/pmc/db/002634.ldb: No such file or directory, value_size:4
      return false;
    if (repairAttempted.getAndSet(true))
      return false;
    if (isClosed)
      throw new IllegalStateException();
    // TODO check message?
    reopenLock.close();
    closeIterators();
    NativeBridge.dbClose(ptr); ptr = 0;
    long ms = SystemClock.uptimeMillis();
    onError(String.format(Locale.US, "Repairing database, because of corruption, path: %s", path), error);
    NativeBridge.dbRepair(this, path);
    openDatabase();
    reopenLock.open();
    onError(String.format(Locale.US, "Repairing database finished successfully in %dms", SystemClock.uptimeMillis() - ms), null);
    return true;
  }

  private long ptr () {
    /*
      FIXME

      It's still possible that dbClose may happen during the active database operation,
      which may lead to race condition use-after-free.

      It seems there's no way to avoid it except making all LevelDB calls synchronized, which is not acceptable,
      because this way calls made on the main thread will be affected by all background operations with the database.

      However, this isn't an issue, because in reality this never happens,
      unless client flush() is frequently
     */
    if (reopenLock != null)
      reopenLock.block();
    return ptr;
  }

  public void close () {
    if (!isClosed) {
      if (batchPtr != 0)
        NativeBridge.dbBatchDestroy(batchPtr);
      closeIterators();
      long ptr = ptr();
      if (ptr != 0) {
        NativeBridge.dbClose(ptr);
      }
      isClosed = true;
    }
  }

  @Override
  protected void finalize () throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  // LevelDB-specific

  public long getSize () {
    if (isClosed)
      throw new IllegalStateException();
    return NativeBridge.dbGetSize(ptr());
  }

  public long getSizeByPrefix (@NonNull String keyPrefix) {
    if (isClosed)
      throw new IllegalStateException();
    return NativeBridge.dbGetSizeByPrefix(ptr(), keyPrefix);
  }

  public @Nullable String getProperty (@NonNull String propertyName) { // e.g. "leveldb.stats", "leveldb.approximate-memory-usage"
    if (isClosed)
      throw new IllegalStateException();
    return NativeBridge.dbGetProperty(ptr(), propertyName);
  }

  public long getValueSize (String key) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetValueSize(ptr(), key, false);
    } catch (Throwable error) {
      if (repair(error)) {
        return getValueSize(key);
      } else {
        onFatalError(error);
        throw error instanceof RuntimeException ? (RuntimeException) error : new RuntimeException(error);
      }
    }
  }

  // Entry search utils

  public byte[][] findAll (@NonNull String keyPrefix) {
    if (isClosed)
      throw new IllegalStateException();
    return NativeBridge.dbFindAll(ptr(), keyPrefix);
    /*try {
      return NativeBridge.dbFindAll(ptr(), keyPrefix);
    } catch (Throwable error) {
      if (repair(error)) {
        return findAll(keyPrefix);
      } else {
        onFatalError(error);
        throw error;
      }
    }*/
  }

  public @Nullable String findByValue (@NonNull String keyPrefix, @NonNull byte[] value) {
    if (isClosed)
      throw new IllegalStateException();
    return NativeBridge.dbFindByValue(ptr(), keyPrefix, value);
    /*try {
      return NativeBridge.dbFindByValue(ptr(), keyPrefix, value);
    } catch (Throwable error) {
      if (repair(error)) {
        return findByValue(keyPrefix, value);
      } else {
        onFatalError(error);
        throw error;
      }
    }*/
  }

  public Iterable<Entry> find (@NonNull String keyPrefix) {
    if (isClosed)
      throw new IllegalStateException();
    return new Iterator(keyPrefix);
  }

  public String findFirst (@NonNull String keyPrefix) {
    for (Entry entry : find(keyPrefix)) {
      String key = entry.key();
      entry.release();
      return key;
    }
    return null;
  }

  // Iterator

  private final List<Entry> openIterators = new ArrayList<>();

  /**
   * Represents database entry.
   *
   * Never keep reference to the entry, because it is reused and updated within the iteratioNativeBridge.
   * Instead do whatever you need with the value returned by {@link #key(),#asInt(),#asLong(),#asFloat(),#asDouble(),#asByteArray(),#asString()}
   *
   * Common usage scenario:
   * for (LevelDB.Entry entry : db.find("keyPrefix")) {
   *   // Do some work
   * }
   *
   * Make sure, whenever you need to break a loop, you call {@link #release()} before exiting the loop:
   * for (LevelDB.Entry entry : db.find("keyPrefix")) {
   *   if (someCondition) {
   *     entry.release();
   *     break;
   *   }
   *   // Do some work
   * }
   */
  public final class Entry {
    private long ptr;
    private @Nullable String key;

    private Entry (long ptr) {
      synchronized (openIterators) {
        this.ptr = ptr;
        openIterators.add(this);
      }
    }

    private void reset (long ptr) {
      if (this.ptr != ptr)
        throw new AssertionError();
      this.ptr = ptr;
      this.key = null;
    }

    @Override
    protected void finalize () throws Throwable {
      try {
        release();
      } finally {
        super.finalize();
      }
    }

    /**
     * Releases all native resources used within the entry.
     *
     * When you call this method, database iterator will implicitly finish its iteratioNativeBridge.
     */
    public void release () {
      synchronized (openIterators) {
        if (ptr != 0) {
          NativeBridge.dbFindFinish(ptr);
          ptr = 0;
          openIterators.remove(this);
        }
      }
    }

    /**
     * @return Full key of current entry
     */
    public String key () {
      if (ptr == 0)
        throw new IllegalStateException();
      return key != null ? key : (key = NativeBridge.dbNextKey(ptr));
    }

    /**
     * @return Boolean representation of current entry
     */
    public boolean asBoolean () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsBoolean(ptr);
    }

    /**
     * @return Int representation of current entry
     */
    public int asInt () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsInt(ptr);
    }

    /**
     * @return Long representation of current entry
     */
    public long asLong () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsLong(ptr);
    }

    /**
     * @return Float representation of current entry
     */
    public float asFloat () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsFloat(ptr);
    }

    /**
     * @return Double representation of current entry
     */
    public double asDouble () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsDouble(ptr);
    }

    /**
     * @return ByteArray representation of current entry
     */
    public byte[] asByteArray () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsByteArray(ptr);
    }

    /**
     * @return LongArray representation of current entry
     */
    public long[] asLongArray () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsLongArray(ptr);
    }

    /**
     * @return String representation of current entry
     */
    public String asString  () {
      if (ptr == 0)
        throw new IllegalStateException();
      return NativeBridge.dbAsString(ptr);
    }
  }

  public final class Iterator implements java.util.Iterator<Entry>, Iterable<Entry> {
    private final String keyPrefix;
    private Entry entry;
    private boolean finished;

    private Iterator (@NonNull String keyPrefix) {
      this.keyPrefix = keyPrefix;
    }

    @NonNull
    @Override
    public java.util.Iterator<Entry> iterator () {
      return this;
    }

    @Override
    public boolean hasNext () {
      if (finished)
        return false;
      if (isClosed) {
        if (entry != null)
          entry.release();
        throw new AssertionError();
      }
      final long ptr;
      if (entry == null) {
        ptr = NativeBridge.dbFind(ptr(), keyPrefix, 0);
        if (ptr == 0) {
          finished = true;
          return false;
        }
        entry = new Entry(ptr);
      } else {
        ptr = entry.ptr != 0 ? NativeBridge.dbFind(ptr(), null, entry.ptr) : 0;
        if (ptr == 0) {
          entry.ptr = 0;
          entry = null;
          finished = true;
          return false;
        }
        entry.reset(ptr);
      }
      return true;
    }

    @Override
    public Entry next () {
      return entry;
    }

    /**
     * Stops iteratioNativeBridge. Further calls to {@link #hasNext()} will return false.
     */
    public void stop () {
      if (entry != null) {
        entry.release();
      }
    }
  }

  // Impl

  @Override
  public boolean contains (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    return NativeBridge.dbContains(ptr(), key);
    /*try {
      return NativeBridge.dbContains(ptr(), key);
    } catch (Throwable error) {
      if (repair(error)) {
        return contains(key);
      }
      onFatalError(error);
      throw error;
    }*/
  }

  public long getIntOrLong (@NonNull String key, int defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetIntOrLong(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getIntOrLong(key, defValue);
      } else {
        onFatalError(error);
        throw error;
      }
    }
  }

  @Override
  public int getInt (@NonNull String key, int defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetInt(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getInt(key, defValue);
      } else {
        onFatalError(error);
        throw error;
      }
    }
  }

  public int tryGetInt (@NonNull String key) throws FileNotFoundException, IllegalStateException {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetInt(ptr(), key, 0, true);
    } catch (FileNotFoundException | IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return tryGetInt(key);
      } else {
        onFatalError(error);
        throw error;
      }
    }
  }

  public @Nullable int[] getIntArray (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetIntArray(ptr(), key);
    } catch (IllegalStateException e) {
      Log.e(LOG_TAG, "Unexpected value format", e);
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getIntArray(key);
      } else {
        onFatalError(error);
        throw error;
      }
    }
  }

  @Override
  public long getLong (@NonNull String key, long defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetLong(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getLong(key, defValue);
      } else {
        onFatalError(error);
        throw error;
      }
    }
  }

  public long tryGetLong (@NonNull String key) throws FileNotFoundException, IllegalStateException {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetLong(ptr(), key, 0, true);
    } catch (FileNotFoundException | IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return tryGetLong(key);
      } else {
        onFatalError(error);
        throw error;
      }
    }
  }

  public @Nullable long[] getLongArray (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetLongArray(ptr(), key);
    } catch (Throwable error) {
      if (repair(error)) {
        return getLongArray(key);
      } else {
        onFatalError(error);
        throw error;
      }
    }
  }

  @Override
  public boolean getBoolean (@NonNull String key, boolean defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetBoolean(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getBoolean(key, defValue);
      }
      onFatalError(error);
      throw error;
    }
  }

  public boolean tryGetBoolean (@NonNull String key) throws FileNotFoundException, IllegalStateException {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetBoolean(ptr(), key, false, true);
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return tryGetBoolean(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  public @Nullable byte[] getByteArray (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetByteArray(ptr(), key);
    } catch (Throwable error) {
      if (repair(error)) {
        return getByteArray(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  @Override
  public float getFloat (@NonNull String key, float defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetFloat(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getFloat(key, defValue);
      }
      onFatalError(error);
      throw error;
    }
  }

  public float tryGetFloat (@NonNull String key) throws FileNotFoundException, IllegalStateException {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetFloat(ptr(), key, 0, true);
    } catch (FileNotFoundException | IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return tryGetFloat(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  public @Nullable float[] getFloatArray (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetFloatArray(ptr(), key);
    } catch (Throwable error) {
      if (repair(error)) {
        return getFloatArray(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  public double getDouble (@NonNull String key, double defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetDouble(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getDouble(key, defValue);
      }
      onFatalError(error);
      throw error;
    }
  }

  public double tryGetDouble (@NonNull String key) throws FileNotFoundException, IllegalStateException {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetDouble(ptr(), key, 0, true);
    } catch (FileNotFoundException | IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return tryGetDouble(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  public @Nullable double[] getDoubleArray (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetDoubleArray(ptr(), key);
    } catch (Throwable error) {
      if (repair(error)) {
        return getDoubleArray(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  public byte getByte (@NonNull String key, byte defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetByte(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getByte(key, defValue);
      }
      onFatalError(error);
      throw error;
    }
  }

  public byte tryGetByte (@NonNull String key) throws FileNotFoundException, IllegalStateException {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetByte(ptr(), key, (byte) 0, true);
    } catch (FileNotFoundException | IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return tryGetByte(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  @Override
  public @Nullable String getString (@NonNull String key, @Nullable String defValue) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetString(ptr(), key, defValue, false);
    } catch (FileNotFoundException e) {
      return defValue;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return getString(key, defValue);
      }
      onFatalError(error);
      throw error;
    }
  }

  @NonNull
  public String tryGetString (@NonNull String key) throws FileNotFoundException, IllegalStateException {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetString(ptr(), key, null, true);
    } catch (FileNotFoundException | IllegalStateException e) {
      throw e;
    } catch (Throwable error) {
      if (repair(error)) {
        return tryGetString(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  @Nullable
  public String[] getStringArray (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    try {
      return NativeBridge.dbGetStringArray(ptr(), key);
    } catch (Throwable error) {
      if (repair(error)) {
        return getStringArray(key);
      }
      onFatalError(error);
      throw error;
    }
  }

  @Override
  public @Nullable Set<String> getStringSet (@NonNull String key, @Nullable Set<String> defValues) {
    if (defValues != null)
      throw new UnsupportedOperationException();
    if (isClosed)
      throw new IllegalStateException();
    String[] array = getStringArray(key);
    if (array == null) {
      return null;
    }
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
      android.util.ArraySet<String> result = new android.util.ArraySet<>(array.length);
      if (array.length > 0) {
        Collections.addAll(result, array);
      }
      return result;
    } else {
      if (array.length == 0) {
        return new HashSet<>(0);
      }
      List<String> list = Arrays.asList(array);
      return new HashSet<>(list);
    }
  }

  // SharedPreferences.Editor

  @Override
  public LevelDB edit () {
    if (semaphore != null) {
      try {
        semaphore.acquire();
      } catch (InterruptedException t) {
        throw new RuntimeException(t);
      }
    }
    synchronized (editLock) {
      if (isEditing)
        throw new AssertionError();
      isEditing = true;
      return this;
    }
  }

  @Override
  public Editor clear () {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbBatchClear(batchPtr, ptr());
        return this;
      }
      try {
        NativeBridge.dbClear(ptr());
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return clear();
    } else {
      onFatalError(error);
      return this;
    }
  }

  @Override
  public Editor remove (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbBatchRemove(batchPtr, key);
        return this;
      }
      try {
        NativeBridge.dbRemove(ptr(), key);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return remove(key);
    } else {
      onFatalError(error);
      return this;
    }
  }

  /**
   * Removes entries by key prefix
   *
   * @param keyPrefix Key prefix
   * @return number of items removed
   */
  public int removeByPrefix (@NonNull String keyPrefix) {
    if (isClosed)
      throw new IllegalStateException();
    if (keyPrefix == null || keyPrefix.isEmpty())
      throw new IllegalArgumentException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing)
        return NativeBridge.dbRemoveByPrefix(ptr(), batchPtr, keyPrefix);
      try {
        return NativeBridge.dbRemoveByPrefix(ptr(), 0, keyPrefix);
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return removeByPrefix(keyPrefix);
    } else {
      onFatalError(error);
      return -1;
    }
  }

  public int removeByAnyPrefix (@NonNull List<String> keyPrefixes) {
    if (keyPrefixes == null || keyPrefixes.isEmpty())
      throw new IllegalArgumentException();
    if (keyPrefixes.size() == 1)
      return removeByPrefix(keyPrefixes.get(0));
    String[] array = new String[keyPrefixes.size()];
    keyPrefixes.toArray(array);
    return removeByAnyPrefix(array);
  }

  public int removeByAnyPrefix (@NonNull String... keyPrefixes) {
    if (keyPrefixes == null || keyPrefixes.length == 0)
      throw new IllegalArgumentException();
    if (keyPrefixes.length == 1)
      return removeByPrefix(keyPrefixes[0]);
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing)
        return NativeBridge.dbRemoveByAnyPrefix(ptr(), batchPtr, keyPrefixes);
      try {
        return NativeBridge.dbRemoveByAnyPrefix(ptr(), 0, keyPrefixes);
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return removeByAnyPrefix(keyPrefixes);
    } else {
      onFatalError(error);
      return -1;
    }
  }

  public Editor putVoid (@NonNull String key) {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutVoid(batchPtr, true, key);
        return this;
      }
      try {
        NativeBridge.dbPutVoid(ptr(), false, key);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putVoid(key);
    } else {
      onFatalError(error);
      return this;
    }
  }

  @Override
  public Editor putInt (@NonNull String key, int value) {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutInt(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutInt(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putInt(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putIntArray (@NonNull String key, @NonNull int[] value) {
    if (value == null)
      throw new IllegalArgumentException();
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutIntArray(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutIntArray(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putIntArray(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  @Override
  public Editor putLong (@NonNull String key, long value) {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutLong(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutLong(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putLong(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putLongArray (@NonNull String key, @NonNull long[] value) {
    if (value == null)
      throw new IllegalArgumentException();
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutLongArray(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutLongArray(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putLongArray(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  @Override
  public Editor putBoolean (@NonNull String key, boolean value) {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutBoolean(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutBoolean(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putBoolean(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  @Override
  public Editor putFloat (@NonNull String key, float value) {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutFloat(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutFloat(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putFloat(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putFloatArray (@NonNull String key, @NonNull float[] value) {
    if (value == null)
      throw new IllegalArgumentException();
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutFloatArray(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutFloatArray(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putFloatArray(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putDouble (@NonNull String key, double value) {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutDouble(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutDouble(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putDouble(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putDoubleArray (@NonNull String key, double[] value) {
    if (value == null)
      throw new IllegalArgumentException();
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutDoubleArray(batchPtr, isEditing, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutDoubleArray(ptr(), isEditing, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putDoubleArray(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putByte (@NonNull String key, byte value) {
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutByte(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutByte(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putByte(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putByteArray (@NonNull String key, @NonNull byte[] value) {
    if (value == null)
      throw new IllegalArgumentException();
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutByteArray(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutByteArray(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putByteArray(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  @Override
  public Editor putString (@NonNull String key, @NonNull String value) {
    if (value == null)
      throw new IllegalArgumentException();
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutString(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutString(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putString(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  public Editor putStringArray (@NonNull String key, @NonNull String[] value) {
    if (value == null)
      throw new IllegalArgumentException();
    if (isClosed)
      throw new IllegalStateException();
    Throwable error;
    synchronized (editLock) {
      if (isEditing) {
        NativeBridge.dbPutStringArray(batchPtr, true, key, value);
        return this;
      }
      try {
        NativeBridge.dbPutStringArray(ptr(), false, key, value);
        return this;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (repair(error)) {
      return putStringArray(key, value);
    } else {
      onFatalError(error);
      return this;
    }
  }

  @Override
  public Editor putStringSet (@NonNull String key, @NonNull Set<String> values) {
    String[] array = new String[values.size()];
    values.toArray(array);
    return putStringArray(key, array);
  }

  @Override
  public boolean commit () {
    return saveChanges();
  }

  @Override
  public void apply () {
    saveChanges();
  }

  private boolean saveChanges () {
    if (isClosed)
      throw new IllegalStateException();
    Throwable error = null;
    synchronized (editLock) {
      if (!isEditing)
        return true;
      try {
        NativeBridge.dbBatchPerform(batchPtr, ptr());
        isEditing = false;
      } catch (Throwable t) {
        error = t;
      }
    }
    if (error != null) {
      if (repair(error)) {
        return saveChanges();
      } else {
        onFatalError(error);
        return false;
      }
    }
    if (semaphore != null)
      semaphore.release();
    return true;
  }

  // Version

  public static String getVersion () {
    return NativeBridge.dbVersion();
  }

  // Unsupported

  @Override
  public final Map<String, ?> getAll () {
    // It's not possible, because type of the value is not stored
    throw new UnsupportedOperationException();
  }

  @Override
  public final void registerOnSharedPreferenceChangeListener (OnSharedPreferenceChangeListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void unregisterOnSharedPreferenceChangeListener (OnSharedPreferenceChangeListener listener) {
    throw new UnsupportedOperationException();
  }
}
