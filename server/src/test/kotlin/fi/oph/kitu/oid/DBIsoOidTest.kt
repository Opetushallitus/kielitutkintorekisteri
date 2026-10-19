package fi.oph.kitu.oid

import fi.oph.kitu.DBContainerConfiguration
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.postgresql.PostgreSQLContainer
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class DBIsoOidTest(
    @param:Autowired private val jdbcTemplate: JdbcTemplate,
    @param:Autowired private val postgres: PostgreSQLContainer,
) {
    @BeforeEach
    fun nukeDb() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS oid_test")
        jdbcTemplate.execute(
            """
            |CREATE TABLE oid_test (
            |    raw iso_oid NOT NULL,
            |    henkilo henkilo_oid NOT NULL,
            |    organisaatio organisaatio_oid NOT NULL
            |)
            """.trimMargin(),
        )
    }

    private fun insert(oid: String) {
        jdbcTemplate.execute(
            """
                |INSERT INTO oid_test (raw, henkilo, organisaatio) VALUES (
                |    '$oid',
                |    '$oid',
                |    '$oid'
                |)
            """.trimMargin(),
        )
    }

    @ParameterizedTest
    @ValueSource(
        strings = ["", ".", "oid", "o.i.d", "1.", "1..2", "1.2..3.4", "a.b.c.d.e.f.g.h", "1.2.246.562.10.1234567890."],
    )
    fun `inserting a malformed OID fails`(malformed: String) {
        assertThrows<DataIntegrityViolationException> {
            insert(malformed)
        }
    }

    @ParameterizedTest
    @ValueSource(
        strings = ["1.2.246.562.10.1234567890", "1", "1.2", "1.2.3.4.5.6.7.8.0"],
    )
    fun `inserting a valid OID succeeds`(valid: String) {
        assertDoesNotThrow {
            insert(valid)
        }
    }

    @ParameterizedTest
    @ValueSource(
        strings = ["1.2.246.562.10.1234567890", "1", "1.2", "1.2.3.4.5.6.7.8.0"],
    )
    fun `inserted OIDs can be successfully read`(oid: String) {
        insert(oid)

        val results =
            jdbcTemplate.query("SELECT * FROM oid_test") { rs, _ ->
                TestRow(
                    rs.getString("raw"),
                    rs.getString("henkilo"),
                    rs.getString("organisaatio"),
                )
            }

        assertContentEquals(listOf(TestRow(oid, oid, oid)), results)
        assertEquals(1, results.size)
    }

    data class TestRow(
        val raw: String,
        val henkilo: String = raw,
        val organisaatio: String = raw,
    )
}
