package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableColumn
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.yki.YkiSuorituksetParams
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class YkiSuoritusColumnTest {
    private val listViewHeaders =
        DisplayTableColumn
            .of<YkiSuoritusColumn, YkiSuoritusEntity>(setOf(ColumnTag.LIST_VIEW))
            .map { it.label }

    private val csvHeaders =
        DisplayTableColumn
            .of<YkiSuoritusColumn, YkiSuoritusEntity>(setOf(ColumnTag.CSV_EXPORT))
            .map { it.label }

    @Test
    fun `henkilotunnus ei nay listanakymassa mutta on mukana CSV-viennissa`() {
        assertFalse(
            listViewHeaders.contains(
                UiText.Yki.Sarake.henkilotunnus
                    .toString(),
            ),
            "Henkilötunnus ei saa näkyä HTML-listanäkymässä",
        )
        assertContains(
            csvHeaders,
            UiText.Yki.Sarake.henkilotunnus
                .toString(),
        )
    }

    @Test
    fun `arviointitila nakyy listanakymassa`() {
        assertContains(
            listViewHeaders,
            UiText.Yki.Sarake.arviointitila
                .toString(),
        )
    }

    @Test
    fun `rekisteriintuontiaika on mukana CSV-viennissa`() {
        assertContains(
            csvHeaders,
            UiText.Yki.Sarake.rekisteriintuontiaika
                .toString(),
        )
    }

    @Test
    fun `rekisteriintuontiaika nakyy listanakymassa`() {
        assertContains(
            listViewHeaders,
            UiText.Yki.Sarake.rekisteriintuontiaika
                .toString(),
        )
    }

    @Test
    fun `solki-tunniste on CSV-viennin viimeinen sarake myos ilman versiohistoriaa`() {
        val solkiTunniste =
            UiText.Yki.Sarake.solkiTunniste
                .toString()

        listOf(false, true).forEach { versionHistory ->
            val headers =
                RenderableDisplayTableEnum
                    .getByTags<YkiSuoritusColumn, YkiSuoritusEntity>(
                        setOf(ColumnTag.CSV_EXPORT),
                        YkiSuorituksetParams(versionHistory = versionHistory).excludeTags(),
                    ).map { it.uiHeaderValue.toString() }

            assertEquals(solkiTunniste, headers.last(), "versionHistory=$versionHistory")
            assertEquals(1, headers.count { it == solkiTunniste }, "versionHistory=$versionHistory")
        }
    }

    @Test
    fun `solki-tunniste ei ole CSV-viennissa kun henkilotiedot piilotetaan`() {
        val headers =
            RenderableDisplayTableEnum
                .getByTags<YkiSuoritusColumn, YkiSuoritusEntity>(
                    setOf(ColumnTag.CSV_EXPORT),
                    YkiSuorituksetParams(piilotaHenkilotiedot = true).excludeTags(),
                ).map { it.uiHeaderValue.toString() }

        assertFalse(
            headers.contains(
                UiText.Yki.Sarake.solkiTunniste
                    .toString(),
            ),
        )
    }

    @Test
    fun `solki-tunniste nakyy listanakymassa vain versiohistoriassa`() {
        fun listHeaders(versionHistory: Boolean) =
            DisplayTableColumn
                .of<YkiSuoritusColumn, YkiSuoritusEntity>(
                    setOf(ColumnTag.LIST_VIEW),
                    YkiSuorituksetParams(versionHistory = versionHistory).excludeTags(),
                ).map { it.label }

        val solkiTunniste =
            UiText.Yki.Sarake.solkiTunniste
                .toString()
        assertFalse(listHeaders(versionHistory = false).contains(solkiTunniste))
        assertEquals(1, listHeaders(versionHistory = true).count { it == solkiTunniste })
    }

    @Test
    fun `oppijanumerosarake on tyhja kun oppijanumeroa ei ole`() {
        val oppijanumeroton = generateRandomYkiSuoritusEntity().copy(suorittajanOID = null)
        val oppijanumerollinen = generateRandomYkiSuoritusEntity()

        assertEquals("", YkiSuoritusColumn.SuorittajanOid.getValue(oppijanumeroton))
        assertEquals(
            oppijanumerollinen.suorittajanOID.toString(),
            YkiSuoritusColumn.SuorittajanOid.getValue(oppijanumerollinen),
        )
    }
}
