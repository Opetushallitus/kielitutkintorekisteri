package fi.oph.kitu.koski

import com.fasterxml.jackson.annotation.JsonFormat
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koodisto.KoskiKoodiviite
import fi.oph.kitu.tietosisalto.Kuvaus
import java.time.LocalDate

sealed interface Osasuoritus {
    @Kuvaus("Tutkinnon osan tyyppi")
    val tyyppi: Koodisto.SuorituksenTyyppi

    @Kuvaus("Suoritettu tutkinnon osa")
    val koulutusmoduuli: OsasuorituksenKoulutusmoduuli

    @Kuvaus("Tutkinnon osan arviointi")
    val arviointi: List<Arvosana>

    @Kuvaus("Tutkintopäivä")
    val alkamispäivä: LocalDate?
}

data class OsasuorituksenKoulutusmoduuli(
    @Kuvaus("Tutkinnon osan nimi")
    val tunniste: KoskiKoodiviite,
)

data class Arvosana(
    @Kuvaus("Arvosana")
    val arvosana: KoskiKoodiviite,
    @Kuvaus("Arviointipäivä")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    val päivä: LocalDate,
)

data class YkiOsasuoritus(
    override val tyyppi: Koodisto.SuorituksenTyyppi = Koodisto.SuorituksenTyyppi.YleisenKielitutkinnonOsa,
    override val koulutusmoduuli: OsasuorituksenKoulutusmoduuli,
    override val arviointi: List<Arvosana>,
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    override val alkamispäivä: LocalDate?,
) : Osasuoritus

data class VktKielitaito(
    override val tyyppi: Koodisto.SuorituksenTyyppi = Koodisto.SuorituksenTyyppi.ValtionhallinnonKielitaito,
    @Kuvaus("Suoritettu kielitaito")
    override val koulutusmoduuli: OsasuorituksenKoulutusmoduuli,
    @Kuvaus("Kielitaidon arviointi", lisatieto = "Arviointi on johdettu kielitaidon osakokeiden arvosanoista.")
    override val arviointi: List<Arvosana>,
    @Kuvaus("Kielitaidon osakokeet")
    val osasuoritukset: List<VktOsakoe>,
    @Kuvaus("Kielitaidon osakokeiden varhaisin tutkintopäivä")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    override val alkamispäivä: LocalDate?,
) : Osasuoritus

data class VktOsakoe(
    @Kuvaus("Osakokeen tyyppi")
    override val tyyppi: Koodisto.SuorituksenTyyppi = Koodisto.SuorituksenTyyppi.ValtionhallinnonKielitutkinnonOsakoe,
    @Kuvaus("Suoritettu osakoe")
    override val koulutusmoduuli: OsasuorituksenKoulutusmoduuli,
    @Kuvaus("Osakokeen arviointi")
    override val arviointi: List<Arvosana>,
    @Kuvaus("Osakokeen tutkintopäivä")
    @param:JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    override val alkamispäivä: LocalDate?,
) : Osasuoritus
