package fi.oph.kitu.yki.historia

import fi.oph.kitu.jdbc.orderSql
import fi.oph.kitu.jdbc.pageSql
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class YkiHistoriaSiirtymatonRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    @WithSpan
    fun haeSivullinen(params: YkiHistoriaSiirtymatonParams): List<YkiHistoriaSiirtymatonEntity> =
        hae(params, params.toOrder())

    @WithSpan
    fun haeKaikki(params: YkiHistoriaSiirtymatonParams): List<YkiHistoriaSiirtymatonEntity> =
        hae(params, params.toCsvOrder())

    @WithSpan
    fun laske(params: YkiHistoriaSiirtymatonParams): Int =
        jdbc.queryForObject(
            """
            SELECT count(*) FROM yki_historia_siirtymaton
            ${params.whereSql().orEmpty()}
            """.trimIndent(),
            MapSqlParameterSource(params.sqlParams()),
            Int::class.java,
        ) ?: 0

    private fun hae(
        params: YkiHistoriaSiirtymatonParams,
        order: YkiHistoriaSiirtymatonOrder,
    ): List<YkiHistoriaSiirtymatonEntity> =
        jdbc.query(
            """
            SELECT * FROM yki_historia_siirtymaton
            ${params.whereSql().orEmpty()}
            ORDER BY ${order.orderSql()}, id
            ${order.pageSql().orEmpty()}
            """.trimIndent(),
            MapSqlParameterSource(params.sqlParams()),
            YkiHistoriaSiirtymatonEntity.fromRow,
        )
}
