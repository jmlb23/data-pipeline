plugins {
    kotlin("jvm") version "2.4.10"
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.apache.flink:flink-streaming-java:2.3.0")
}

group = "com.github.jmlb23.data"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    compilerOptions {
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21
        javaParameters = true
    }
}
