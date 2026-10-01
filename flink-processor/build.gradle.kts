plugins {
    kotlin("jvm") version "2.4.10"
    application
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.apache.flink:flink-connector-kafka:5.0.0-2.2")
    implementation("org.apache.flink:flink-streaming-java:2.3.0")
}

group = "com.github.jmlb23.data"
version = "1.0.0-SNAPSHOT"

application {
    mainClass = "com.github.jmlb23.data.flink.Application"
}

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
