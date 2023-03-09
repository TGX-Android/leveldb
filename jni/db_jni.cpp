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
 * File created on 04/18/2018
 */

#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>
#include <jni.h>
#include <vector>
#include <algorithm>
#include <android/log.h>
#include <unistd.h>
#include <jni_utils.h>

// DB

#define ERROR_PREFIX std::string(__func__) + ":" + std::to_string(__LINE__)
#define MODULO_MESSAGE(A,B) "(" + std::to_string(A) + " % " + std::to_string(B) + ") != 0"
#define NEQ_MESSAGE(A,B) std::to_string(A) + " != " + std::to_string(B)
#define LESS_MESSAGE(A,B) std::to_string(A) + " < " + std::to_string(B)

#define LOG_TAG "LevelDBJni"


// #define ON_ERROR(...) jni::onDatabaseError(env, ERROR_PREFIX, __VA_ARGS__)

inline std::string makeErrorText (const std::string &prefix, const std::string &message, const std::string &key = "") {
  return key.empty() ? prefix + ": " + message : prefix + ": " + message + ", key:" + key;
}

#define ON_ERROR(...) onFatalError(env, jDatabase, makeErrorText(ERROR_PREFIX, __VA_ARGS__))
#define ON_RECOVERABLE_ERROR(...) jni::throw_new(env, makeErrorText(ERROR_PREFIX, __VA_ARGS__), jni_class::AssertionError(env))
#define ON_ARGUMENT_ERROR(...) jni::throw_new(env, makeErrorText(ERROR_PREFIX, __VA_ARGS__), jni_class::IllegalArgumentException(env))
#define ON_VALUE_ERROR(...) jni::throw_new(env, makeErrorText(ERROR_PREFIX, __VA_ARGS__), jni_class::IllegalStateException(env))

#define DB_FUNC(RETURN_TYPE, NAME, ...)                              \
  extern "C" {                                                        \
  JNIEXPORT RETURN_TYPE                                               \
      Java_me_vkryl_leveldb_NativeBridge_##NAME( \
          JNIEnv *env, jclass clazz, ##__VA_ARGS__);                  \
  }                                                                   \
  JNIEXPORT RETURN_TYPE                                               \
      Java_me_vkryl_leveldb_NativeBridge_##NAME( \
          JNIEnv *env, jclass clazz, ##__VA_ARGS__)

inline leveldb::DB *get_database(jlong ptr) {
  return jni::jlong_to_ptr<leveldb::DB *>(ptr);
}
inline leveldb::WriteBatch *get_batch (jlong ptr) {
  return jni::jlong_to_ptr<leveldb::WriteBatch *>(ptr);
}

void onFatalError (JNIEnv *env, jobject database, const std::string &error) {
  jclass cls = env->GetObjectClass(database);
  jmethodID mid = env->GetMethodID(cls, "onFatalError", "(Ljava/lang/String;)V");
  if (mid != nullptr) {
    jstring jError = jni::to_jstring(env, error);
    env->CallVoidMethod(database, mid, jError);
    if (jError) {
      env->DeleteLocalRef(jError);
    }
  } else {
    jni::throw_new(env, error, jni_class::AssertionError(env));
  }
}

#define KEY_NOT_FOUND_EXCEPTION jni_class::FileNotFoundException(env)

leveldb::Options make_db_options () {
  leveldb::Options options;
  options.create_if_missing = true;
  options.reuse_logs = true;
  options.max_open_files = 50;
  options.write_buffer_size = 2u << 15u; // 4KB
  options.block_cache = leveldb::NewLRUCache(2u << 15u); // .5MB
  return options;
}

DB_FUNC(jstring, dbVersion) {
  auto version = std::to_string(leveldb::kMajorVersion) + "." + std::to_string(leveldb::kMinorVersion);
  return jni::to_jstring(env, version);
}

DB_FUNC(jboolean, dbRepair, jobject jDatabase, jstring jPath) {
  std::string path = jni::from_jstring(env, jPath);
  leveldb::Options options = make_db_options();
  leveldb::Status status = leveldb::RepairDB(path, options);
  if (!status.ok()) {
    ON_ERROR(status.ToString());
    return JNI_FALSE;
  }
  return JNI_TRUE;
}

DB_FUNC(jlong, dbOpen, jobject jDatabase, jstring jPath) {
  std::string path = jni::from_jstring(env, jPath);
  leveldb::Options options = make_db_options();

  leveldb::DB *db = nullptr;

  leveldb::Status status;
  size_t attempt_count = 0;
  do {
    status = leveldb::DB::Open(options, path, &db);
    if (!status.ok() && status.IsIOError() && status.ToString().find("Try again") != std::string::npos) {
      size_t total_wait = 100 * attempt_count++;
      if (total_wait < 5000 /*5 seconds max*/) {
        usleep(100000);
        continue;
      } else {
        ON_ERROR(status.ToString() + ", total_wait: " + std::to_string(total_wait) + "ms");
        return 0;
      }
    }
    break;
  } while (true);
  if (!status.ok()) {
    bool ok = false;
    std::string error = status.ToString();
    __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, "dbOpen open: %s, attempting recover", error.c_str());
    status = leveldb::RepairDB(path, options);
    if (!status.ok()) {
      __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, "dbOpen recover: %s", status.ToString().c_str());
    } else {
      __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, "dbOpen recover: ok");
      status = leveldb::DB::Open(options, path, &db);
      if (!status.ok()) {
        error = status.ToString();
      } else {
        ok = true;
      }
    }
    if (!ok) {
      __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, "dbOpen open: %s, abort", error.c_str());
      ON_ERROR(error);
      return 0;
    }
  }

  return jni::ptr_to_jlong(db);
}

DB_FUNC(void, dbClose, jlong ptr) {
  leveldb::DB *db = get_database(ptr);
  delete db;
}

DB_FUNC(jlong, dbGetSize, jlong ptr) {
  leveldb::DB *db = get_database(ptr);
  leveldb::ReadOptions options;
  options.fill_cache = false;
  leveldb::Iterator *itr = db->NewIterator(options);
  size_t count = 0;
  for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
    count++;
  }
  delete itr;
  return count;
}

DB_FUNC(jlong, dbGetSizeByPrefix, jlong ptr, jstring jKeyPrefix) {
  std::string prefix = jni::from_jstring(env, jKeyPrefix);
  if (prefix.empty()) {
    ON_ARGUMENT_ERROR("prefix.empty()");
    return -1;
  }
  leveldb::DB *db = get_database(ptr);
  leveldb::Iterator *itr = db->NewIterator(leveldb::ReadOptions());
  size_t count = 0;
  for (itr->Seek(prefix); itr->Valid(); itr->Next()) {
    if (itr->key().ToString().compare(0, prefix.size(), prefix) == 0) {
      count++;
    } else {
      break;
    }
  }
  return count;
}

DB_FUNC(jstring, dbGetProperty, jlong ptr, jstring jPropertyName) {
  std::string name = jni::from_jstring(env, jPropertyName);
  leveldb::DB *db = get_database(ptr);
  std::string value;
  if (!db->GetProperty(name, &value)) {
    ON_ARGUMENT_ERROR("Unknown property", name);
    return nullptr;
  }
  return jni::to_jstring(env, value);
}

DB_FUNC(jobjectArray, dbFindAll, jlong ptr, jstring jKeyPrefix)  {
  std::string prefix = jni::from_jstring(env, jKeyPrefix);
  if (prefix.empty()) {
    ON_ARGUMENT_ERROR("prefix.empty()");
    return nullptr;
  }
  leveldb::DB *db = get_database(ptr);
  leveldb::Iterator *itr = db->NewIterator(leveldb::ReadOptions());
  std::vector<leveldb::Slice> values;
  for (itr->Seek(prefix); itr->Valid(); itr->Next()) {
    auto const &key = itr->key();
    if (key.ToString().compare(0, prefix.size(), prefix) != 0) {
      break;
    }
    auto const &value = itr->value();
    if ((value.size() % sizeof(jbyte)) != 0) {
      ON_VALUE_ERROR(MODULO_MESSAGE(value.size(), sizeof(jbyte)), key.ToString());
    } else {
      values.push_back(value);
    }
  }
  delete itr;
  if (values.empty()) {
    return nullptr;
  }
  jclass jclass_ByteArray = jni_class::ByteArray(env);
  jobjectArray result = env->NewObjectArray(values.size(), jclass_ByteArray, nullptr);
  jsize index = 0;
  for (auto const& value : values) {
    jbyteArray array = env->NewByteArray(value.size());
    size_t length = value.size() / sizeof(jbyte);
    if (length > 0) {
      env->SetByteArrayRegion(array, 0, length, (const jbyte *) value.data());
    }
    env->SetObjectArrayElement(result, index, array);
    env->DeleteLocalRef(array);
    index++;
  }
  return result;
}

DB_FUNC(jstring, dbFindByValue, jlong ptr, jstring jKeyPrefix, jbyteArray jValue) {
  std::string prefix = jni::from_jstring(env, jKeyPrefix);
  if (prefix.empty()) {
    ON_ARGUMENT_ERROR("prefix.empty()");
    return nullptr;
  }
  jsize length = env->GetArrayLength(jValue);
  jbyte *elements = nullptr;
  leveldb::Slice value;
  if (length > 0) {
    elements = env->GetByteArrayElements(jValue, nullptr);
    if (elements == nullptr) {
      ON_ARGUMENT_ERROR("unable to get elements", prefix);
      return nullptr;
    }
    value = leveldb::Slice((const char *) elements, length * sizeof(jbyte));
  }
  leveldb::DB *db = get_database(ptr);
  leveldb::Iterator *itr = db->NewIterator(leveldb::ReadOptions());
  std::string found_key;
  for (itr->Seek(prefix); itr->Valid(); itr->Next()) {
    auto const &key = itr->key();
    if (key.ToString().compare(0, prefix.size(), prefix) != 0) {
      break;
    }
    auto const &data = itr->value();
    if (value.size() == data.size() && (data.size() == 0 || memcmp(value.data(), data.data(), data.size()) == 0)) {
      found_key = key.ToString();
      break;
    }
  }
  delete itr;
  if (elements != nullptr) {
    env->ReleaseByteArrayElements(jValue, elements, JNI_ABORT);
  }
  if (!found_key.empty()) {
    return env->NewStringUTF(found_key.c_str());
  }
  return nullptr;
}

struct DatabaseIterator {
  const std::string prefix;
  leveldb::Iterator *itr;

  DatabaseIterator(leveldb::DB *db, const std::string &prefix) : itr(db->NewIterator(leveldb::ReadOptions())), prefix(prefix) {
    itr->Seek(prefix);
  }
  ~DatabaseIterator() {
    delete itr;
  }

  bool next () {
    itr->Next();
    return is_valid();
  }

  bool is_valid () {
    if (itr->Valid()) {
      auto const &key = itr->key();
      if (key.ToString().compare(0, prefix.size(), prefix) == 0) {
        return true;
      }
    }
    return false;
  }
};
inline DatabaseIterator *get_iterator(jlong ptr) {
  return jni::jlong_to_ptr<DatabaseIterator *>(ptr);
}

DB_FUNC(jlong, dbFind, jlong ptr, jstring jKeyPrefix, jlong iteratorPtr) {
  leveldb::DB *db = get_database(ptr);
  DatabaseIterator *iterator = get_iterator(iteratorPtr);
  bool ok;
  if (iterator != nullptr) {
    if (jKeyPrefix != nullptr) {
      ON_ARGUMENT_ERROR("!prefix.empty()");
      return 0;
    }
    ok = iterator->next();
  } else {
    std::string prefix = jni::from_jstring(env, jKeyPrefix);
    if (prefix.empty()) {
      ON_ARGUMENT_ERROR("prefix.empty()");
      return 0;
    }
    iterator = new DatabaseIterator(db, prefix);
    ok = iterator->is_valid();
  }
  if (ok) {
    return jni::ptr_to_jlong(iterator);
  } else {
    delete iterator;
    return 0;
  }
}

DB_FUNC(void, dbFindFinish, jlong iteratorPtr) {
  DatabaseIterator *iterator = get_iterator(iteratorPtr);
  delete iterator;
}

DB_FUNC(jstring, dbNextKey, jlong iteratorPtr) {
  DatabaseIterator *iterator = get_iterator(iteratorPtr);
  if (iterator != nullptr)
    return env->NewStringUTF(iterator->itr->key().ToString().c_str());
  return nullptr;
}

#define DB_FUNC_CAST(RETURN_TYPE, NAME) \
DB_FUNC(RETURN_TYPE, NAME, jlong iteratorPtr) { \
  DatabaseIterator *iterator = get_iterator(iteratorPtr); \
  auto const &value = iterator->itr->value(); \
  size_t size = value.size(); \
  if (size != sizeof(RETURN_TYPE)) { \
    ON_VALUE_ERROR(NEQ_MESSAGE(size, sizeof(RETURN_TYPE)), iterator->itr->key().ToString()); \
    return 0; \
  } \
  return jni::as<RETURN_TYPE>(value.data()); \
}

DB_FUNC_CAST(jboolean, dbAsBoolean)
DB_FUNC_CAST(jint, dbAsInt)
DB_FUNC_CAST(jlong, dbAsLong)
DB_FUNC_CAST(jfloat, dbAsFloat)
DB_FUNC_CAST(jdouble, dbAsDouble)

#define DB_FUNC_CAST_ARRAY(TYPE, NAME, ARRAY_TYPE) \
DB_FUNC(TYPE##Array, NAME, jlong iteratorPtr) { \
  DatabaseIterator *iterator = get_iterator(iteratorPtr); \
  auto const &value = iterator->itr->value(); \
  if ((value.size() % sizeof(TYPE)) != 0) { \
    ON_VALUE_ERROR(MODULO_MESSAGE(value.size(), sizeof(TYPE)), iterator->itr->key().ToString()); \
    return nullptr; \
  } \
  jsize length = value.size() / sizeof(TYPE); \
  TYPE##Array result = env->New##ARRAY_TYPE##Array(length); \
  if (env->ExceptionCheck() == JNI_TRUE) { \
    return nullptr; \
  } \
  if (length > 0) { \
    env->Set ##ARRAY_TYPE## ArrayRegion(result, 0, length, (const TYPE *) value.data()); \
  } \
  return result; \
}

DB_FUNC_CAST_ARRAY(jbyte, dbAsByteArray, Byte)
DB_FUNC_CAST_ARRAY(jlong, dbAsLongArray, Long)

DB_FUNC(jstring, dbAsString, jlong iteratorPtr) {
  DatabaseIterator *iterator = get_iterator(iteratorPtr);
  auto const &value = iterator->itr->value();
  if ((value.size() % sizeof(jchar)) != 0) {
    ON_VALUE_ERROR(MODULO_MESSAGE(value.size(), sizeof(jchar)), iterator->itr->key().ToString());
    return nullptr;
  }
  auto length = (jsize) (value.size() / sizeof(jchar));
  if (length > 0) {
    return env->NewString((jchar *) value.data(), length);
  } else {
    return env->NewStringUTF("");
  }
}

// Basic ops

DB_FUNC(jboolean, dbClear, jlong ptr) {
  leveldb::DB *db = get_database(ptr);
  leveldb::WriteBatch batch;
  leveldb::ReadOptions options;
  options.fill_cache = false;
  leveldb::Iterator *itr = db->NewIterator(options);
  for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
    batch.Delete(itr->key());
  }
  delete itr;
  leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
  if (!s.ok()) {
    ON_RECOVERABLE_ERROR(s.ToString());
    return JNI_FALSE;
  }
  return JNI_TRUE;
}

DB_FUNC(jboolean, dbRemove, jlong ptr, jstring jKey) {
  std::string key = jni::from_jstring(env, jKey);
  leveldb::DB *db = get_database(ptr);
  leveldb::Status s = db->Delete(leveldb::WriteOptions(), key);
  if (!s.ok() && !s.IsNotFound()) {
    ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key);
    return JNI_FALSE;
  }
  return JNI_TRUE;
}

DB_FUNC(jint, dbRemoveByPrefix, jlong ptr, jlong batchPtr, jstring jKeyPrefix) {
  std::string prefix = jni::from_jstring(env, jKeyPrefix);
  if (prefix.empty()) {
    ON_ARGUMENT_ERROR("prefix.empty()");
    return -1;
  }
  leveldb::DB *db = get_database(ptr);
  leveldb::Iterator *itr = db->NewIterator(leveldb::ReadOptions());
  leveldb::WriteBatch *batch = get_batch(batchPtr);
  size_t removed_count = 0;
  for (itr->Seek(prefix); itr->Valid(); itr->Next()) {
    auto const &key = itr->key();
    if (key.ToString().compare(0, prefix.size(), prefix) == 0) {
      if (batch == nullptr) {
        batch = new leveldb::WriteBatch();
      }
      batch->Delete(key);
      removed_count++;
    } else {
      break;
    }
  }
  delete itr;
  if (removed_count == 0 || batchPtr != 0) {
    return (jint) removed_count;
  }
  leveldb::Status s = db->Write(leveldb::WriteOptions(), batch);
  if (batchPtr == 0) {
    delete batch;
  }
  if (!s.ok()) {
    ON_RECOVERABLE_ERROR(s.ToString());
    return -1;
  }
  return (jint) removed_count;
}

DB_FUNC(jint, dbRemoveByAnyPrefix, jlong ptr, jlong batchPtr, jobjectArray jKeyPrefixes) {
  jsize length = env->GetArrayLength(jKeyPrefixes);
  if (length == 0) {
    ON_ARGUMENT_ERROR("length == 0");
    return -1;
  }
  std::vector<std::string> prefixes;
  for (jsize i = 0; i < length; i++) {
    jstring string = (jstring) (env->GetObjectArrayElement(jKeyPrefixes, i));
    jsize stringLength = env->GetStringLength(string);
    if (stringLength > 0) {
      const char *bytes = env->GetStringUTFChars(string, JNI_FALSE);
      if (bytes != nullptr) {
        std::string keyPrefix(bytes);
        prefixes.push_back(keyPrefix);
        env->ReleaseStringUTFChars(string, bytes);
      }
    }
    env->DeleteLocalRef(string);
  }
  if (prefixes.empty()) {
    ON_ARGUMENT_ERROR("prefixes.empty()");
    return -1;
  }
  std::sort(prefixes.begin(), prefixes.end());

  leveldb::DB *db = get_database(ptr);
  leveldb::WriteBatch *batch = get_batch(batchPtr);

  leveldb::Iterator* itr = db->NewIterator(leveldb::ReadOptions());
  size_t removed_count = 0;
  for (const auto &prefix : prefixes) {
    for (itr->Seek(prefix); itr->Valid(); itr->Next()) {
      auto const &key = itr->key();
      if (key.ToString().compare(0, prefix.size(), prefix) == 0) {
        if (batch == nullptr) {
          batch = new leveldb::WriteBatch();
        }
        batch->Delete(key);
        removed_count++;
      } else {
        break;
      }
    }
  }
  if (removed_count == 0 || batchPtr != 0) {
    return removed_count;
  }
  leveldb::Status s = db->Write(leveldb::WriteOptions(), batch);
  if (batchPtr == 0) {
    delete batch;
  }
  if (!s.ok()) {
    ON_RECOVERABLE_ERROR(s.ToString());
    return -1;
  }
  return removed_count;
}

DB_FUNC(jboolean, dbContains, jlong ptr, jstring jKey) {
  std::string key = jni::from_jstring(env, jKey);
  leveldb::DB *db = get_database(ptr);
  leveldb::Iterator* itr = db->NewIterator(leveldb::ReadOptions());
  itr->Seek(key);
  bool found = itr->Valid() && itr->key().ToString() == key;
  delete itr;
  if (found) {
    return JNI_TRUE;
  } else {
    return JNI_FALSE;
  }
}

// Simple Getters

template <typename T>
T dbParse (JNIEnv *env, const std::string &key, const std::string &value, T defaultValue, jboolean throwIfError) {
  size_t size = value.size();
  if (size != sizeof(T)) {
    ON_VALUE_ERROR(NEQ_MESSAGE(size, sizeof(T)), key);
    /*if (throwIfError == JNI_TRUE) {
      jni::throw_new(env, "Bad value", KEY_NOT_FOUND_EXCEPTION);
    }*/
    return defaultValue;
  }
  return jni::as<T>(value.data());
}

template <>
jstring dbParse<jstring> (JNIEnv *env, const std::string &key, const std::string &value, jstring defaultValue, jboolean throwIfError) {
  size_t size = value.size();
  if ((size % sizeof(jchar)) != 0) {
    ON_VALUE_ERROR(MODULO_MESSAGE(size, sizeof(jchar)), key);
    /*if (throwIfError == JNI_TRUE) {
      jni::throw_new(env, "Bad value", KEY_NOT_FOUND_EXCEPTION);
    }*/
    return defaultValue;
  }
  jsize length = value.size() / sizeof(jchar);
  if (length > 0) {
    return env->NewString((jchar *) value.data(), length);
  } else {
    return env->NewStringUTF("");
  }
}

DB_FUNC(jlong, dbGetValueSize, jlong ptr, jstring jKey, jboolean throwIfError) {
  std::string key = jni::from_jstring(env, jKey);
  leveldb::DB *db = get_database(ptr);
  std::string value;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
  if (!s.ok()) {
    if (!s.IsNotFound()) {
      ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key);
    }
    if (throwIfError == JNI_TRUE) {
      jni::throw_new(env, s.ToString(), KEY_NOT_FOUND_EXCEPTION);
    }
    return -1;
  }
  return value.size();
}

DB_FUNC(jlong, dbGetIntOrLong, jlong ptr, jstring jKey, jint defaultValue, jboolean throwIfError) {
  std::string key = jni::from_jstring(env, jKey);
  leveldb::DB *db = get_database(ptr);
  std::string value;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
  if (!s.ok()) {
    if (!s.IsNotFound()) {
      ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key);
    }
    if (throwIfError == JNI_TRUE) {
      jni::throw_new(env, s.ToString(), KEY_NOT_FOUND_EXCEPTION);
    }
    return defaultValue;
  }
  size_t size = value.size();
  if (size == sizeof(jint)) {
    return (jlong) dbParse<jint>(env, key, value, defaultValue, throwIfError);
  } else if (size == sizeof(jlong)) {
    return dbParse<jlong>(env, key, value, (jlong) defaultValue, throwIfError);
  }
  ON_VALUE_ERROR(NEQ_MESSAGE(size, sizeof(jlong)), key);
  return defaultValue;
}

template <typename T>
T dbGet (JNIEnv *env, jlong ptr, jstring jKey, T defaultValue, jboolean throwIfError) {
  std::string key = jni::from_jstring(env, jKey);
  leveldb::DB *db = get_database(ptr);
  std::string value;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
  if (!s.ok()) {
    if (!s.IsNotFound()) {
      ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key);
    }
    if (throwIfError == JNI_TRUE) {
      jni::throw_new(env, s.ToString(), KEY_NOT_FOUND_EXCEPTION);
    }
    return defaultValue;
  }
  return dbParse<T>(env, key, value, defaultValue, throwIfError);
}

#define DB_FUNC_GET(RETURN_TYPE, NAME) \
DB_FUNC(RETURN_TYPE, NAME, jlong ptr, jstring jKey, RETURN_TYPE defaultValue, jboolean throwIfError) { \
  return dbGet<RETURN_TYPE>(env, ptr, jKey, defaultValue, throwIfError); \
}

DB_FUNC_GET(jint, dbGetInt); // "%d"
DB_FUNC_GET(jlong, dbGetLong); // "%lld"
DB_FUNC_GET(jboolean, dbGetBoolean); // "%hu"
DB_FUNC_GET(jbyte, dbGetByte); // "%hu"
DB_FUNC_GET(jfloat, dbGetFloat); // "%f"
DB_FUNC_GET(jdouble, dbGetDouble); // "%f"
DB_FUNC_GET(jstring, dbGetString); // "%p"

// Array Getters

template <typename T, typename ARRAY_T>
ARRAY_T dbParseArray (JNIEnv *env, const std::string &key, const std::string &value) {
  if ((value.size() % sizeof(T)) != 0) {
    ON_VALUE_ERROR(MODULO_MESSAGE(value.size(), sizeof(T)), key);
    return nullptr;
  }
  jsize length = value.size() / sizeof(T);
  ARRAY_T result = jni::array_new<ARRAY_T>(env, length);
  if (env->ExceptionCheck() == JNI_TRUE) {
    return nullptr;
  }
  if (length > 0) {
    jni::array_set<ARRAY_T>(env, result, 0, length, value.data());
  }
  return result;
}

template<>
jobjectArray dbParseArray<jstring,jobjectArray> (JNIEnv *env, const std::string &key, const std::string &value) {
  if (value.size() < sizeof(jsize)) {
    ON_VALUE_ERROR(LESS_MESSAGE(value.size(), sizeof(jsize)), key);
    return nullptr;
  }
  size_t position = 0;
  size_t remaining = value.size();
  if (remaining < sizeof(jsize)) {
    ON_VALUE_ERROR(LESS_MESSAGE(remaining, sizeof(jsize)));
    return nullptr;
  }

  jsize size = jni::as<jsize>(value.data());
  position += sizeof(jsize);
  remaining -= sizeof(jsize);

  jclass jclass_String = jni_class::String(env);
  jobjectArray result = env->NewObjectArray(size, jclass_String, nullptr);
  if (env->ExceptionCheck() == JNI_TRUE) {
    return nullptr;
  }

  for (jsize i = 0; i < size && remaining > 0; i++) {
    if (remaining < sizeof(jsize)) {
      ON_VALUE_ERROR(LESS_MESSAGE(remaining, sizeof(jsize)), key);
      return nullptr;
    }
    jsize length = jni::as<jsize>(value.data() + position);
    position += sizeof(jsize);
    remaining -= sizeof(jsize);

    if (length < 0) {
      ON_VALUE_ERROR(LESS_MESSAGE(length, 0), key);
      return nullptr;
    }
    if (remaining < (length * sizeof(jchar))) {
      ON_VALUE_ERROR(std::to_string(remaining) + " < (" + std::to_string(length) + " * " + std::to_string(sizeof(jchar)) + ")", key);
      return nullptr;
    }
    jstring string;
    if (length > 0) {
      string = env->NewString((jchar *) (value.data() + position), length);
    } else {
      string = env->NewStringUTF("");
    }
    env->SetObjectArrayElement(result, i, string);
    env->DeleteLocalRef(string);

    position += length * sizeof(jchar);
    remaining -= length * sizeof(jchar);
  }
  return result;
}

template <typename T, typename ARRAY_T>
ARRAY_T dbGetArray (JNIEnv *env, jlong ptr, jstring jKey) {
  std::string key = jni::from_jstring(env, jKey);
  leveldb::DB *db = get_database(ptr);
  std::string value;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
  if (!s.ok()) {
    if (!s.IsNotFound()) {
      ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key);
    }
    return nullptr;
  }
  return dbParseArray<T,ARRAY_T>(env, key, value);
}

#define DB_FUNC_GET_ARRAY(TYPE, NAME) \
DB_FUNC(TYPE##Array, NAME, jlong ptr, jstring jKey) { \
  return dbGetArray<TYPE,TYPE##Array>(env, ptr, jKey); \
}

DB_FUNC_GET_ARRAY(jint, dbGetIntArray);
DB_FUNC_GET_ARRAY(jlong, dbGetLongArray);
DB_FUNC_GET_ARRAY(jbyte, dbGetByteArray);
DB_FUNC_GET_ARRAY(jfloat, dbGetFloatArray);
DB_FUNC_GET_ARRAY(jdouble, dbGetDoubleArray);
DB_FUNC(jobjectArray, dbGetStringArray, jlong ptr, jstring jKey) {
  return dbGetArray<jstring, jobjectArray>(env, ptr, jKey);
}

// Simple Setters

jboolean dbPut(JNIEnv *env, jlong ptr, jboolean isBatch, const std::string &key, const leveldb::Slice &value) {
  if (isBatch == JNI_TRUE) {
    leveldb::WriteBatch *batch = get_batch(ptr);
    batch->Put(key, value);
  } else {
    leveldb::DB *db = get_database(ptr);
    leveldb::Status s = db->Put(leveldb::WriteOptions(), key, value);
    if (!s.ok()) {
      ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key + ", value_size:" + std::to_string(value.size()));
      return JNI_FALSE;
    }
  }
  return JNI_TRUE;
}

#define DB_FUNC_PUT(TYPE, NAME) \
DB_FUNC(jboolean, NAME, jlong ptr, jboolean isBatch, jstring jKey, TYPE jValue) { \
  std::string key = jni::from_jstring(env, jKey); \
  leveldb::Slice value((const char *) &jValue, sizeof(TYPE)); \
  return dbPut(env, ptr, isBatch, key, value); \
}
DB_FUNC(jboolean, dbPutVoid, jlong ptr, jboolean isBatch, jstring jKey) {
  std::string key = jni::from_jstring(env, jKey);
  return dbPut(env, ptr, isBatch, key, leveldb::Slice());
}
DB_FUNC_PUT(jint, dbPutInt);
DB_FUNC_PUT(jlong, dbPutLong);
DB_FUNC_PUT(jboolean, dbPutBoolean);
DB_FUNC_PUT(jbyte, dbPutByte);
DB_FUNC_PUT(jfloat, dbPutFloat);
DB_FUNC_PUT(jdouble, dbPutDouble);
DB_FUNC(jboolean, dbPutString, jlong ptr, jboolean isBatch, jstring jKey, jstring jValue) {
  std::string key = jni::from_jstring(env, jKey);
  jsize length = env->GetStringLength(jValue);
  const jchar *buffer = length > 0 ? env->GetStringChars(jValue, nullptr) : nullptr;
  if (buffer == nullptr && length > 0) {
    return JNI_FALSE;
  }
  jboolean result;
  if (length > 0) {
    result = dbPut(env, ptr, isBatch, key, leveldb::Slice((char *) buffer, (size_t) length * sizeof(jchar)));
  } else {
    result = dbPut(env, ptr, isBatch, key, leveldb::Slice());
  }
  if (buffer != nullptr) {
    env->ReleaseStringChars(jValue, buffer);
  }
  return result;
}

// Array Setters

template <typename T,typename ARRAY_T>
jboolean dbPutArray (JNIEnv *env, jlong ptr, jboolean isBatch, jstring jKey, ARRAY_T jValue) {
  std::string key = jni::from_jstring(env, jKey);
  jsize length = env->GetArrayLength(jValue);
  T *elements = nullptr;
  jboolean result;
  if (length > 0) {
    elements = jni::array_get<T>(env, jValue);
    if (elements == nullptr) {
      ON_ARGUMENT_ERROR("elements == nullptr", key);
      return JNI_FALSE;
    }
    result = dbPut(env, ptr, isBatch, key, leveldb::Slice((const char *) elements, length * sizeof(T)));
    jni::array_release<T,ARRAY_T>(env, jValue, elements);
  } else {
    result = dbPut(env, ptr, isBatch, key, leveldb::Slice());
  }
  return result;
}

#define DB_FUNC_PUT_ARRAY(TYPE, NAME, ARRAY_TYPE) \
DB_FUNC(jboolean, NAME, jlong ptr, jboolean isBatch, jstring jKey, TYPE##Array jValue) { \
  std::string key = jni::from_jstring(env, jKey); \
  jsize length = env->GetArrayLength(jValue); \
  leveldb::Slice value; \
  TYPE *elements = nullptr; \
  if (length > 0) { \
    elements = env->Get##ARRAY_TYPE##ArrayElements(jValue, nullptr); \
    if (elements == nullptr) { \
      ON_ARGUMENT_ERROR("elements == nullptr", key); \
      return JNI_FALSE; \
    } \
    value = leveldb::Slice((const char *) elements, length * sizeof(TYPE)); \
  } \
  jboolean result = JNI_TRUE; \
  if (isBatch == JNI_TRUE) { \
    leveldb::WriteBatch *batch = get_batch(ptr); \
    batch->Put(key, value); \
  } else { \
    leveldb::DB *db = get_database(ptr); \
    leveldb::Status s = db->Put(leveldb::WriteOptions(), key, value); \
    if (!s.ok()) { \
      ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key); \
      result = JNI_FALSE; \
    } \
  } \
  if (elements != nullptr) { \
    env->Release##ARRAY_TYPE##ArrayElements(jValue, elements, JNI_ABORT); \
  } \
  return result; \
}

DB_FUNC_PUT_ARRAY(jint, dbPutIntArray, Int);
DB_FUNC_PUT_ARRAY(jlong, dbPutLongArray, Long);
DB_FUNC_PUT_ARRAY(jbyte, dbPutByteArray, Byte);
DB_FUNC_PUT_ARRAY(jfloat, dbPutFloatArray, Float);
DB_FUNC_PUT_ARRAY(jdouble, dbPutDoubleArray, Double);

DB_FUNC(jboolean, dbPutStringArray, jlong ptr, jboolean isBatch, jstring jKey, jobjectArray jValue) {
  std::string key = jni::from_jstring(env, jKey);
  jsize length = env->GetArrayLength(jValue);
  size_t buffer_size = sizeof(jsize);
  for (jsize i = 0; i < length; i++) {
    jstring string = (jstring) (env->GetObjectArrayElement(jValue, i));
    jsize stringLength = env->GetStringLength(string);
    buffer_size += sizeof(jsize) + stringLength * sizeof(jchar);
    env->DeleteLocalRef(string);
  }
  char *buffer = (char *) malloc(buffer_size);
  if (buffer == nullptr) {
    ON_ARGUMENT_ERROR("malloc failed for size " + std::to_string(buffer_size), key);
    return JNI_FALSE;
  }
  // *((jsize *) buffer) = length;
  std::memcpy(buffer, &length, sizeof(jsize));

  size_t position = sizeof(jsize);
  for (jsize i = 0; i < length; i++) {
    jstring string = (jstring) (env->GetObjectArrayElement(jValue, i));
    jsize stringLength = env->GetStringLength(string);

    // *((jsize *) (buffer + position)) = stringLength;
    std::memcpy(buffer + position, &stringLength, sizeof(jsize));

    position += sizeof(jsize);
    if (stringLength > 0) {
      env->GetStringRegion(string, 0, stringLength, (jchar *) (buffer + position));
    }
    env->DeleteLocalRef(string);
    position += stringLength * sizeof(jchar);
  }
  jboolean result = JNI_TRUE;
  leveldb::Slice value(buffer, buffer_size);
  if (isBatch == JNI_TRUE) {
    leveldb::WriteBatch *batch = get_batch(ptr);
    batch->Put(key, value);
  } else {
    leveldb::DB *db = get_database(ptr);
    leveldb::Status s = db->Put(leveldb::WriteOptions(), key, value);
    if (!s.ok()) {
      ON_RECOVERABLE_ERROR(s.ToString() + ", key:" + key + ", buffer_size:" + std::to_string(buffer_size));
      result = JNI_FALSE;
    }
  }
  free(buffer);
  return result;
}

// Batch

DB_FUNC(jlong, dbBatchCreate) {
  return jni::ptr_to_jlong(new leveldb::WriteBatch());
}

DB_FUNC(jboolean, dbBatchPerform, jlong ptr, jlong databasePtr) {
  leveldb::WriteBatch *batch = get_batch(ptr);
  leveldb::DB *db = get_database(databasePtr);
  leveldb::Status s = db->Write(leveldb::WriteOptions(), batch);
  if (!s.ok()) {
    ON_RECOVERABLE_ERROR(s.ToString());
    return JNI_FALSE;
  }
  batch->Clear();
  return JNI_TRUE;
}

DB_FUNC(void, dbBatchDestroy, jlong ptr) {
  leveldb::WriteBatch *batch = get_batch(ptr);
  delete batch;
}

DB_FUNC(void, dbBatchClear, jlong ptr, jlong databasePtr) {
  leveldb::WriteBatch *batch = get_batch(ptr);
  batch->Clear();
  leveldb::DB *db = get_database(databasePtr);
  leveldb::ReadOptions options;
  options.fill_cache = false;
  leveldb::Iterator *itr = db->NewIterator(options);
  for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
    batch->Delete(itr->key());
  }
  delete itr;
}

DB_FUNC(jboolean, dbBatchRemove, jlong ptr, jstring jKey) {
  std::string key = jni::from_jstring(env, jKey);
  leveldb::WriteBatch *batch = get_batch(ptr);
  batch->Delete(key);
  return JNI_TRUE;
}