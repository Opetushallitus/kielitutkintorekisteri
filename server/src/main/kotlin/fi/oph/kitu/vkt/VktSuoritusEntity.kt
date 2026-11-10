package fi.oph.kitu.vkt

import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import fi.oph.kitu.tiedontuontischema.VktSuoritus
import fi.oph.kitu.util.IgnoreForEquality
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.MappedCollection
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime

@Table(name = "vkt_suoritus")
data class VktSuoritusEntity(
    @Id
    @IgnoreForEquality("VKT")
    val id: Int? = null,
    val ilmoittautumisenId: String,
    val suorittajanOid: Oid,
    val etunimet: String,
    val sukunimi: String,
    val tutkintokieli: Koodisto.Tutkintokieli,
    val suorituspaikkakunta: String?,
    val taitotaso: Koodisto.VktTaitotaso,
    val suorituksenVastaanottaja: Oid?,
    @MappedCollection(idColumn = "suoritus_id")
    val osakokeet: Set<VktOsakoe>,
    @MappedCollection(idColumn = "suoritus_id")
    val tutkinnot: Set<VktTutkinto>,
    @IgnoreForEquality("VKT")
    val createdAt: OffsetDateTime? = null,
    val koskiOpiskeluoikeus: Oid? = null,
    @IgnoreForEquality("VKT")
    val koskiSiirtoKasitelty: Boolean = false,
) {
    fun kitunTiedotSailyttaen(tallennettu: VktSuoritusEntity): VktSuoritusEntity =
        copy(
            koskiOpiskeluoikeus = koskiOpiskeluoikeus ?: tallennettu.koskiOpiskeluoikeus,
            osakokeet =
                if (taitotaso == Koodisto.VktTaitotaso.Erinomainen) {
                    osakokeet.map { it.virkailijanArviointiSailyttaen(tallennettu.osakokeet) }.toSet()
                } else {
                    osakokeet
                },
        )

    @Table(name = "vkt_osakoe")
    data class VktOsakoe(
        @Id
        @IgnoreForEquality("VKT")
        val id: Int? = null,
        val tyyppi: Koodisto.VktOsakoe,
        val tutkintopaiva: LocalDate,
        val arviointipaiva: LocalDate?,
        val arvosana: Koodisto.VktArvosana?,
        val merkittyPoistettavaksi: Instant?,
    ) {
        fun virkailijanArviointiSailyttaen(tallennetut: Set<VktOsakoe>): VktOsakoe {
            if (arvosana != null || merkittyPoistettavaksi != null) return this
            val tallennettu =
                tallennetut.firstOrNull { it.tyyppi == tyyppi && it.tutkintopaiva == tutkintopaiva } ?: return this
            return copy(
                arviointipaiva = tallennettu.arviointipaiva,
                arvosana = tallennettu.arvosana,
                merkittyPoistettavaksi = tallennettu.merkittyPoistettavaksi,
            )
        }
    }

    @Table(name = "vkt_tutkinto")
    data class VktTutkinto(
        @Id
        @IgnoreForEquality("VKT")
        val id: Int? = null,
        val tyyppi: Koodisto.VktKielitaito,
        val arviointipaiva: LocalDate?,
        val arvosana: Koodisto.VktArvosana?,
    )

    fun toHenkilosuoritus(): VktHenkilosuoritus =
        Henkilosuoritus(
            henkilo =
                Henkilo(
                    oid = suorittajanOid,
                    etunimet = etunimet,
                    sukunimi = sukunimi,
                ),
            suoritus =
                VktSuoritus(
                    taitotaso = taitotaso,
                    kieli = tutkintokieli,
                    suorituksenVastaanottaja = suorituksenVastaanottaja,
                    suorituspaikkakunta = suorituspaikkakunta,
                    osat =
                        osakokeet.map {
                            fi.oph.kitu.vkt.VktOsakoe
                                .from(it)
                        },
                    lahdejarjestelmanId = LahdejarjestelmanTunniste.from(ilmoittautumisenId),
                    internalId = id,
                    koskiOpiskeluoikeusOid = koskiOpiskeluoikeus,
                    koskiSiirtoKasitelty = koskiSiirtoKasitelty,
                ),
            lisatty = createdAt,
        )

    companion object {
        fun from(hs: Henkilosuoritus<VktSuoritus>): VktSuoritusEntity {
            val oppija = hs.henkilo
            return with(hs.suoritus) {
                VktSuoritusEntity(
                    ilmoittautumisenId = hs.suoritus.lahdejarjestelmanId.toString(),
                    suorittajanOid = oppija.oid,
                    etunimet = oppija.etunimet.orEmpty(),
                    sukunimi = oppija.sukunimi.orEmpty(),
                    tutkintokieli = kieli,
                    suorituspaikkakunta = suorituspaikkakunta,
                    taitotaso = taitotaso,
                    suorituksenVastaanottaja = suorituksenVastaanottaja,
                    osakokeet = osat.map { it.toVktOsakoeRow() }.toSet(),
                    tutkinnot = tutkinnot.map { it.toVktTutkintoRow() }.toSet(),
                    koskiOpiskeluoikeus = koskiOpiskeluoikeusOid,
                    koskiSiirtoKasitelty = koskiSiirtoKasitelty,
                )
            }
        }
    }
}
