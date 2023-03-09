/*
 * This file is a part of LevelDB Preferences
 * Copyright © Vyacheslav Krylov (slavone@protonmail.ch) 2014-2023
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

import java.io.FileNotFoundException;

class NativeBridge { // db_jni.cpp
  public native static String dbVersion ();

  public native static long dbOpen (LevelDB context, String path);
  public native static long dbRepair (LevelDB context, String path);
  public native static void dbClose (long ptr);
  public native static long dbGetSize (long ptr);
  public native static long dbGetSizeByPrefix (long ptr, String keyPrefix);
  public native static String dbGetProperty (long ptr, String propertyName);

  public native static long dbGetValueSize (long ptr, String key, boolean throwIfError) throws AssertionError, FileNotFoundException;

  public native static byte[][] dbFindAll (long ptr, String keyPrefix);
  public native static String dbFindByValue (long ptr, String keyPrefix, byte[] value);
  public native static long dbFind (long ptr, String keyPrefix, long iteratorPtr);
  public native static void dbFindFinish (long iteratorPtr);

  public native static String dbNextKey (long iteratorPtr);
  public native static boolean dbAsBoolean (long iteratorPtr);
  public native static int dbAsInt (long iteratorPtr);
  public native static long dbAsLong (long iteratorPtr);
  public native static float dbAsFloat (long iteratorPtr);
  public native static double dbAsDouble (long iteratorPtr);
  public native static byte[] dbAsByteArray (long iteratorPtr);
  public native static long[] dbAsLongArray (long iteratorPtr);
  public native static String dbAsString (long iteratorPtr);

  public native static boolean dbClear (long ptr) throws AssertionError;
  public native static boolean dbRemove (long ptr, String key) throws AssertionError;
  public native static int dbRemoveByPrefix (long ptr, long batchPtr, String keyPrefix) throws AssertionError;
  public native static int dbRemoveByAnyPrefix (long ptr, long batchPtr, String[] keyPrefix) throws AssertionError;

  public native static boolean dbContains (long ptr, String key);
  public native static int dbGetIntOrLong (long ptr, String key, int defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;
  public native static int dbGetInt (long ptr, String key, int defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;
  public native static long dbGetLong (long ptr, String key, long defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;
  public native static boolean dbGetBoolean (long ptr, String key, boolean defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;
  public native static byte dbGetByte (long ptr, String key, byte defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;
  public native static float dbGetFloat (long ptr, String key, float defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;
  public native static double dbGetDouble (long ptr, String key, double defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;
  public native static String dbGetString (long ptr, String key, String defaultValue, boolean throwIfError) throws AssertionError, FileNotFoundException;

  public native static int[] dbGetIntArray (long ptr, String key) throws AssertionError;
  public native static long[] dbGetLongArray (long ptr, String key) throws AssertionError;
  public native static byte[] dbGetByteArray (long ptr, String key) throws AssertionError;
  public native static float[] dbGetFloatArray (long ptr, String key) throws AssertionError;
  public native static double[] dbGetDoubleArray (long ptr, String key) throws AssertionError;
  public native static String[] dbGetStringArray (long ptr, String key) throws AssertionError;

  public native static boolean dbPutVoid (long ptr, boolean isBatch, String key) throws AssertionError;
  public native static boolean dbPutInt (long ptr, boolean isBatch, String key, int value) throws AssertionError;
  public native static boolean dbPutLong (long ptr, boolean isBatch, String key, long value) throws AssertionError;
  public native static boolean dbPutBoolean (long ptr, boolean isBatch, String key, boolean value) throws AssertionError;
  public native static boolean dbPutByte (long ptr, boolean isBatch, String key, byte value) throws AssertionError;
  public native static boolean dbPutFloat (long ptr, boolean isBatch, String key, float value) throws AssertionError;
  public native static boolean dbPutDouble (long ptr, boolean isBatch, String key, double value) throws AssertionError;
  public native static boolean dbPutString (long ptr, boolean isBatch, String key, String value) throws AssertionError;

  public native static boolean dbPutIntArray (long ptr, boolean isBatch, String key, int[] value) throws AssertionError;
  public native static boolean dbPutLongArray (long ptr, boolean isBatch, String key, long[] value) throws AssertionError;
  public native static boolean dbPutByteArray (long ptr, boolean isBatch, String key, byte[] value) throws AssertionError;
  public native static boolean dbPutFloatArray (long ptr, boolean isBatch, String key, float[] value) throws AssertionError;
  public native static boolean dbPutDoubleArray (long ptr, boolean isBatch, String key, double[] value) throws AssertionError;
  public native static boolean dbPutStringArray (long ptr, boolean isBatch, String key, String[] value) throws AssertionError;

  public native static long dbBatchCreate ();
  public native static boolean dbBatchPerform (long ptr, long databasePtr) throws AssertionError;
  public native static void dbBatchDestroy (long ptr);

  public native static void dbBatchClear (long ptr, long databasePtr);
  public native static boolean dbBatchRemove (long ptr, String key);
}
