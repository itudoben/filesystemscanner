package org.jh.filesystemscanner

import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 *
 * User: hujol
 * Date: 2019-04-06
 * Time: 10:25
 */
class BasePlugin implements Plugin<Project> {

    public void apply(Project project) {
        project.tasks.create("base", BaseTask) {
            group = 'Base'
            description = 'The base task for the file system scanner project.'
        }
    }
}
