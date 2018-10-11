package com.mconsulting.mschema.cli

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.SystemExitException
import com.xenomachina.argparser.default

class Config(parser: ArgParser) {
    val logging = LoggingConfig(parser)
    val general = GeneralConfig(parser)

    fun validate() {
    }
}

class LoggingConfig(parser: ArgParser) {
    val quiet by parser.flagging("--quiet", help = "Logging: turn off all logging").default(false)
    val verbosity by parser.counting("-v", "--verbosity", help = "Logging: verbosity of logging (repeatable)").default(0)
    val logPath by parser.storing("--logpath", help = "Logging: log file to send write to instead of stdout - has to be a file, not directory").default(null)
}

class GeneralConfig(parser: ArgParser) {
    val version by parser.option<Unit>("--version", help = "General: display the version") {
        throw ShowVersionException("version      : ${App.version}${System.lineSeparator()}git revision : ${App.gitRev}")
    }.default(false)
}

class ShowVersionException(version: String) : SystemExitException(version, 0)
