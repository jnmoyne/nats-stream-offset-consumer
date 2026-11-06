plugins {
    `java-library`
}

group = "io.synadia"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

dependencies {
    api("io.nats:jnats:2.26.4")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("io.nats:jnats-server-runner:4.0.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<JavaExec>("bench") {
    description = "Runs the offset-consumer throughput benchmark (-PbenchArgs=\"--scenario sync-100 --msgs 2000000\")"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("io.synadia.offsetconsumer.Bench")
    args = (project.findProperty("benchArgs") as String?)?.split(" ")?.filter { it.isNotEmpty() } ?: listOf()
}
