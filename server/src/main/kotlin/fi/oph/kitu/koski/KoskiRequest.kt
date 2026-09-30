package fi.oph.kitu.koski

import com.fasterxml.jackson.annotation.JsonFormat
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koodisto.KoskiKoodiviite
import fi.oph.kitu.oid.Oid
import java.time.LocalDate

data class KoskiRequest(
    val henkilö: Henkilo,
    val opiskeluoikeudet: List<Opiskeluoikeus>,
) {
    data class Henkilo(
        val oid: Oid,
    )

    data class Opiskeluoikeus(
        val lähdejärjestelmänId: LahdeJarjestelmanId,
        val tyyppi: Koodisto.OpiskeluoikeudenTyyppi = Koodisto.OpiskeluoikeudenTyyppi.Kielitutkinto,
        val tila: Tila,
        val suoritukset: List<KielitutkintoSuoritus>,
        val oid: Oid? = null,
    ) {
        data class LahdeJarjestelmanId(
            val id: String,
            val lähdejärjestelmä: Koodisto.LahdeJarjestelma = Koodisto.LahdeJarjestelma.Kielitutkintorekisteri,
        )

        data class Tila(
            val opiskeluoikeusjaksot: List<OpiskeluoikeusJakso>,
        ) {
            data class OpiskeluoikeusJakso(
                @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
                val alku: LocalDate,
                val tila: Koodisto.OpiskeluoikeudenTila,
            )
        }

        data class KielitutkintoSuoritus(
            val tyyppi: Koodisto.SuorituksenTyyppi,
            val koulutusmoduuli: KoulutusModuuli,
            val toimipiste: Organisaatio,
            val vahvistus: Vahvistus?,
            val osasuoritukset: List<Osasuoritus>,
            // Seuraavat kentät ovat käytössä yki-suorituksilla:
            val järjestäjä: Organisaatio? = null,
            val yleisarvosana: KoskiKoodiviite? = null,
        ) {
            data class KoulutusModuuli(
                val tunniste: KoskiKoodiviite,
                val kieli: Koodisto.Tutkintokieli,
            )

            data class Organisaatio(
                val oid: Oid,
            )

            interface Vahvistus {
                val päivä: LocalDate
            }

            data class VahvistusImpl(
                @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
                override val päivä: LocalDate,
                val myöntäjäOrganisaatio: Organisaatio,
            ) : Vahvistus

            data class VahvistusPaikkakunnalla(
                @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
                override val päivä: LocalDate,
                val myöntäjäOrganisaatio: Organisaatio,
                val paikkakunta: KoskiKoodiviite, // koodistoUri: kunta
            ) : Vahvistus
        }
    }
}
