package org.jh.filesystemscanner

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

class BaseTask extends DefaultTask {

    @TaskAction
    void doSomething() {
        println 'Base task'
    }
}
