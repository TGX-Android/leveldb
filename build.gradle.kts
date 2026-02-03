plugins {
  id(libs.plugins.android.library.get().pluginId)
  id("tgx-module")
}

dependencies {
  implementation(libs.androidx.annotation)
}

android {
  namespace = "me.vkryl.leveldb"

  defaultConfig {
    consumerProguardFiles("consumer-rules.pro")
  }

  externalNativeBuild {
    cmake {
      path("jni/CMakeLists.txt")
    }
  }

  buildFeatures {
    buildConfig = true
  }
}