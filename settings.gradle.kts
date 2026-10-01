pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
}

rootProject.name="data-lake"

include(
    ":kafka-producer",
    ":flink-processor"
)
