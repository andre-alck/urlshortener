import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream

plugins {
    application
    id("io.github.urlshortenerasacxyz.buildlambdadeploymentzip") version "1.1"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(libs.junit.jupiter)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    implementation(libs.guava)

    implementation("com.amazonaws:aws-lambda-java-core:1.2.3")

    implementation(platform("software.amazon.awssdk:bom:2.29.23"))
    implementation("software.amazon.awssdk:s3")

    implementation(platform("com.fasterxml.jackson:jackson-bom:2.18.2"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = "com.asacxyz.App"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
