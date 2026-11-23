package fi.oph.kitu.koski

import com.fasterxml.jackson.annotation.JsonFormat
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koodisto.KoskiKoodiviite
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tietosisalto.Kuvaus
import java.time.LocalDate

data class KoskiRequest(
    @Kuvaus("Tutkinnon suorittaja")
    val henkilö: Henkilo,
    @Kuvaus("Suorittajan opiskeluoikeus", lisatieto = "Lähetyksessä on aina yksi opiskeluoikeus.")
    val opiskeluoikeudet: List<Opiskeluoikeus>,
) {
    data class Henkilo(
        @Kuvaus("Suorittajan oppijanumero")
        val oid: Oid,
    )

    data class Opiskeluoikeus(
        @Kuvaus("Suorituksen tunnistetiedot")
        val lähdejärjestelmänId: LahdeJarjestelmanId,
        @Kuvaus("Opiskeluoikeuden tyyppi")
        val tyyppi: Koodisto.OpiskeluoikeudenTyyppi = Koodisto.OpiskeluoikeudenTyyppi.Kielitutkinto,
        @Kuvaus("Opiskeluoikeuden tilahistoria")
        val tila: Tila,
        @Kuvaus("Tutkinnon suoritus", lisatieto = "Lähetyksessä on aina yksi tutkinnon suoritus.")
        val suoritukset: List<KielitutkintoSuoritus>,
        @Kuvaus(
            "KOSKI-järjestelmän opiskeluoikeudelle antama tunniste; tyhjä, kun suoritus lähetetään ensimmäisen kerran",
        )
        val oid: Oid? = null,
    ) {
        data class LahdeJarjestelmanId(
            @Kuvaus("Suorituksen tunniste Kielitutkintorekisterissä")
            val id: String,
            @Kuvaus("Järjestelmä, josta tieto on peräisin")
            val lähdejärjestelmä: Koodisto.LahdeJarjestelma = Koodisto.LahdeJarjestelma.Kielitutkintorekisteri,
        )

        data class Tila(
            @Kuvaus("Opiskeluoikeuden tilat aikajärjestyksessä")
            val opiskeluoikeusjaksot: List<OpiskeluoikeusJakso>,
        ) {
            data class OpiskeluoikeusJakso(
                @Kuvaus("Päivä, jona tila alkoi: tutkintopäivä tai arviointipäivä")
                @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
                val alku: LocalDate,
                @Kuvaus("Opiskeluoikeuden tila")
                val tila: Koodisto.OpiskeluoikeudenTila,
            )
        }

        data class KielitutkintoSuoritus(
            @Kuvaus("Suorituksen tyyppi")
            val tyyppi: Koodisto.SuorituksenTyyppi,
            @Kuvaus("Tutkinnon taso ja kieli")
            val koulutusmoduuli: KoulutusModuuli,
            @Kuvaus("Organisaatio, jonka alaisuudessa tutkinto on suoritettu")
            val toimipiste: Organisaatio,
            @Kuvaus("Tutkinnon vahvistus", lisatieto = "Tyhjä, jos arviointipäivää ei ole.")
            val vahvistus: Vahvistus?,
            @Kuvaus("Tutkinnon osat")
            val osasuoritukset: List<Osasuoritus>,
            @Kuvaus("Tutkinnon järjestäjä")
            val järjestäjä: Organisaatio? = null,
            @Kuvaus("Tutkinnon yleisarvosana; tyhjä, jos sitä ei ole annettu")
            val yleisarvosana: KoskiKoodiviite? = null,
        ) {
            data class KoulutusModuuli(
                @Kuvaus("Tutkinnon taso")
                val tunniste: KoskiKoodiviite,
                @Kuvaus("Tutkinnon kieli")
                val kieli: Koodisto.Tutkintokieli,
            )

            data class Organisaatio(
                @Kuvaus("Organisaation tunniste Opintopolun organisaatiopalvelussa")
                val oid: Oid,
            )

            sealed interface Vahvistus {
                @Kuvaus("Päivä, jona tutkinto vahvistettiin: viimeisin arviointipäivä")
                val päivä: LocalDate
            }

            data class VahvistusImpl(
                @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
                override val päivä: LocalDate,
                @Kuvaus("Tutkinnon vahvistanut organisaatio")
                val myöntäjäOrganisaatio: Organisaatio,
            ) : Vahvistus

            data class VahvistusPaikkakunnalla(
                @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
                override val päivä: LocalDate,
                @Kuvaus("Tutkinnon vahvistanut organisaatio")
                val myöntäjäOrganisaatio: Organisaatio,
                @Kuvaus("Kunta, jossa tutkinto suoritettiin")
                val paikkakunta: KoskiKoodiviite,
            ) : Vahvistus
        }
    }
}
