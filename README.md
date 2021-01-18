# LevelDB Preferences

Java wrapper for [Google's LevelDB](https://github.com/google/leveldb) that implements SharedPreferences interface. Additionally, it supports iteration by prefix, array storage and lookup by value.

For API documentation refer to javadoc inside [LevelDB.java](src/main/java/me/vkryl/leveldb/LevelDB.java).

## TODOs

* `registerOnSharedPreferenceChangeListener` and `unregisterOnSharedPreferenceChangeListener` are unsupported, but in case you need them, they should be relatively easy to implement.
* [LevelDB](src/main/java/me/vkryl/leveldb/LevelDB.java) object follows a popular native handle pattern. However, according to [this Google I/O talk](https://www.youtube.com/watch?v=7_caITSjk1k), such objects should be designed in a different way.
* Building is currently dependent on `module-plugin`, `cmake-plugin` and `LibraryVersions.ANNOTATIONS`, which must be defined inside `buildSrc/build.gradle.kts` of the root project.
* Ability to add this library to your project through Maven dependency.

## Licence

`LevelDB Preferences` is licensed under the terms of the Apache License, Version 2.0. See [LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0) for more information.