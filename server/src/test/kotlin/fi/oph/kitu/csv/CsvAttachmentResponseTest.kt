package fi.oph.kitu.csv

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.TolgeeMessages
import fi.oph.kitu.yki.suoritukset.YkiSuoritusColumn
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.context.i18n.LocaleContextHolder
import java.io.ByteArrayOutputStream
import java.util.Locale
import kotlin.test.assertContains

class CsvAttachmentResponseTest {
    @AfterEach
    fun cleanup() {
        TolgeeMessages.set(emptyMap())
        LocaleContextHolder.resetLocaleContext()
    }

    @Test
    fun `CSV-otsikot noudattavat pyynnon kielta myos striimaussaikeessa`() {
        TolgeeMessages.set(mapOf("yki.sarake.oppijanumero" to LocalizedString(sv = "Studentnummer")))
        LocaleContextHolder.setLocale(Locale.of("sv"))

        val response =
            csvAttachmentResponse<YkiSuoritusColumn, YkiSuoritusEntity>(
                filename = "test.csv",
                data = emptyList(),
            )

        LocaleContextHolder.resetLocaleContext()
        val output = ByteArrayOutputStream()
        Thread { response.body!!.writeTo(output) }.apply {
            start()
            join()
        }

        assertContains(output.toString(Charsets.UTF_8), "Studentnummer")
    }
}
