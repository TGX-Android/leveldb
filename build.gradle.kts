plugins {
  id(libs.plugins.android.library.get().pluginId)
  id("tgx-module")
}

dependencies {
  implementation(libs.androidx.annotation)
}

android {
  namespace = "me.vkryl.leveldb"

  externalNativeBuild {
    cmake {
      path("jni/CMakeLists.txt")
    }
  }

  buildFeatures {
    buildConfig = true
  }
}