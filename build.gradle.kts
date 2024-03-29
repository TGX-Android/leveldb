/*
 * This file is a part of LevelDB Preferences
 * Copyright © Vyacheslav Krylov (slavone@protonmail.ch) 2014
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
 * File created on 18/01/2021
 */

@file:Suppress("UnstableApiUsage")

plugins {
    id("com.android.library")
    id("module-plugin")
    id("cmake-plugin")
}

dependencies {
    implementation("androidx.annotation:annotation:${LibraryVersions.ANNOTATIONS}")
}

android {
  namespace = "me.vkryl.leveldb"

  buildFeatures {
    buildConfig = true
  }
}