plugins {
  id(libs.plugins.android.library.get().pluginId)
  alias(libs.plugins.kotlin.android)
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