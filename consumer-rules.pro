-keepclasseswithmembernames,includedescriptorclasses class * {
    native <methods>;
}
-keep class me.vkryl.leveldb.LevelDB {
    void onFatalError(java.lang.String);
}