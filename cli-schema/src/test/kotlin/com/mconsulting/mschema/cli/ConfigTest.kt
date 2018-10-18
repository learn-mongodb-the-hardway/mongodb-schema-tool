package com.mconsulting.mschema.cli

import com.mconsulting.mrelational.schema.extractor.Namespace
import com.xenomachina.argparser.ArgParser
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class ConfigTest {

    @Test
    fun shouldThrowDueToIllegalMongoDBURI() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf("--extract", "--uri", "mong://localhost:27017").toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains("The connection string is invalid"))
    }

    @Test
    fun shouldCorrectlyParseNamespace() {
        val config = ArgParser(listOf(
            "--extract",
            "--uri", "mongodb://localhost:27017",
            "--namespace", "db1.coll:1000"
        ).toTypedArray()).parseInto(::Config)
        config.validate()

        assertEquals(1, config.extract.namespaces.size)
        assertEquals(Namespace("db1", "coll", 1000), config.extract.namespaces.first())
    }

    @Test
    fun shouldFailToParseNamespace() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--extract",
                "--uri", "mongodb://localhost:27017",
                "--namespace", "db1.coll:test"
            ).toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains("--namespace must be of format <db.collection:sampleSize (int = 0 means all)>, ex: [db1.coll:1000]"))
    }

    @Test
    fun shouldFailToLocateDirectory() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--extract",
                "--uri", "mongodb://localhost:27017",
                "--output-directory", ":fdsafdsfsd"
            ).toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains(":fdsafdsfsd is not a valid directory path, directory not found"))
    }

    @Test
    fun shouldFailToParseSchemaInApplyMode() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--apply",
                "--schema", "mongodb://localhost:27017"
            ).toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains("--schema validationLevel [//localhost] is not a valid MongoDB validation level. Please user one of [STRICT, MODERATE]"))
    }

    @Test
    fun shouldFailToParseSchemaInApplyModeDueToIllegalValidationLevel() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--apply",
                "--schema", "db1.coll1:typer:./peter.json"
            ).toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains("--schema validationLevel [typer] is not a valid MongoDB validation level. Please user one of [STRICT, MODERATE]"))
    }

    @Test
    fun shouldFailToParseSchemaInApplyModeDueToIllegalFile() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--apply",
                "--schema", "db1.coll1:strict:./fdsafdsfsd"
            ).toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains("--schema file at ./fdsafdsfsd does not exist or is not a file"))
    }

    @Test
    fun shouldCorrectlyParseApply() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--apply",
                "--schema", "db1.coll1:strict:./fdsafdsfsd:100"
            ).toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains("--schema file at ./fdsafdsfsd:100 does not exist or is not a file"))
    }

    @Test
    fun shouldCorrectlyParseComplexApply() {
        val exception = assertThrows<IllegalArgumentException> {
            ArgParser(listOf(
                "--apply",
                "--schema",
                "quickstart.sights:strict:./quickstart_sights_2018-10-18T10:29Z.json",
                "--schema",
                "quickstart.users:strict:./quickstart_users_2018-10-18T10:29Z.json",
                "--uri",
                "mongodb://localhost:27017"
            ).toTypedArray()).parseInto(::Config).validate()
        }

        assert(exception.message!!.contains("--schema file at ./quickstart_sights_2018-10-18T10:29Z.json does not exist or is not a file"))
    }
}