package fi.oph.kitu.openapi

import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Henkilo
import fi.oph.kitu.tiedontuontischema.Henkilosuoritus
import fi.oph.kitu.tiedontuontischema.Lahdejarjestelma
import fi.oph.kitu.tiedontuontischema.LahdejarjestelmanTunniste
import fi.oph.kitu.tiedontuontischema.TiedonsiirtoFailure
import fi.oph.kitu.tiedontuontischema.TiedonsiirtoSuccess
import fi.oph.kitu.tiedontuontischema.VktSuoritus
import fi.oph.kitu.tiedontuontischema.YkiJarjestaja
import fi.oph.kitu.tiedontuontischema.YkiOsa
import fi.oph.kitu.tiedontuontischema.YkiSuoritus
import fi.oph.kitu.tiedontuontischema.YkiTarkastusarviointi
import fi.oph.kitu.util.defaultObjectMapper
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.vkt.VktArvionti
import fi.oph.kitu.vkt.VktKirjoittamisenKoe
import fi.oph.kitu.vkt.VktPuheenYmmartamisenKoe
import fi.oph.kitu.vkt.VktPuhumisenKoe
import fi.oph.kitu.vkt.VktTekstinYmmartamisenKoe
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.Sukupuoli
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.YkiArvioija
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeus
import fi.oph.kitu.yki.suoritukset.Todistuskieli
import io.swagger.v3.oas.annotations.media.Schema
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.cfg.MapperConfig
import tools.jackson.databind.introspect.AnnotatedMember
import tools.jackson.databind.introspect.JacksonAnnotationIntrospector
import java.time.LocalDate

@Controller
@RequestMapping("/schema-examples")
class SchemaExamplesController {
    @GetMapping("/vkt-erinomainen-ilmoittautuminen.json", produces = ["application/json;charset=UTF-8"])
    fun vktErinomainenResponse() =
        exampleJson(
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.67409348034").getOrThrow(),
                        etunimet = "Eino Testi",
                        sukunimi = "Välimaa-Testi",
                    ),
                suoritus =
                    VktSuoritus(
                        taitotaso = Koodisto.VktTaitotaso.Erinomainen,
                        kieli = Koodisto.Tutkintokieli.FIN,
                        osat =
                            listOf(
                                VktKirjoittamisenKoe(
                                    tutkintopaiva = LocalDate.now(),
                                ),
                                VktTekstinYmmartamisenKoe(
                                    tutkintopaiva = LocalDate.now(),
                                ),
                                VktPuhumisenKoe(
                                    tutkintopaiva = LocalDate.now(),
                                ),
                                VktPuheenYmmartamisenKoe(
                                    tutkintopaiva = LocalDate.now(),
                                ),
                            ),
                        lahdejarjestelmanId = LahdejarjestelmanTunniste.randomFrom(Lahdejarjestelma.OPHTesti),
                    ),
            ),
        )

    @GetMapping("/vkt-hyvajatyydyttava-suoritus.json", produces = ["application/json;charset=UTF-8"])
    fun vktHyvaJaTyydyttavaResponse(): ResponseEntity<ByteArray> {
        val tutkintopaiva = LocalDate.now().minusDays(60)
        val arviointipaiva = LocalDate.now()

        return exampleJson(
            Henkilosuoritus(
                henkilo =
                    Henkilo(
                        oid = Oid.parse("1.2.246.562.24.67409348034").getOrThrow(),
                        etunimet = "Eino Testi",
                        sukunimi = "Välimaa-Testi",
                    ),
                suoritus =
                    VktSuoritus(
                        taitotaso = Koodisto.VktTaitotaso.HyväJaTyydyttävä,
                        kieli = Koodisto.Tutkintokieli.FIN,
                        suorituksenVastaanottaja = Oid.parse("1.2.246.562.24.10691606777").getOrThrow(),
                        suorituspaikkakunta = "091",
                        osat =
                            listOf(
                                VktKirjoittamisenKoe(
                                    tutkintopaiva = tutkintopaiva,
                                    arviointi =
                                        VktArvionti(
                                            arvosana = Koodisto.VktArvosana.Tyydyttävä,
                                            paivamaara = arviointipaiva,
                                        ),
                                ),
                                VktTekstinYmmartamisenKoe(
                                    tutkintopaiva = tutkintopaiva,
                                    arviointi =
                                        VktArvionti(
                                            arvosana = Koodisto.VktArvosana.Hyvä,
                                            paivamaara = arviointipaiva,
                                        ),
                                ),
                                VktPuhumisenKoe(
                                    tutkintopaiva = tutkintopaiva,
                                    arviointi =
                                        VktArvionti(
                                            arvosana = Koodisto.VktArvosana.Tyydyttävä,
                                            paivamaara = arviointipaiva,
                                        ),
                                ),
                                VktPuheenYmmartamisenKoe(
                                    tutkintopaiva = tutkintopaiva,
                                    arviointi =
                                        VktArvionti(
                                            arvosana = Koodisto.VktArvosana.Hylätty,
                                            paivamaara = arviointipaiva,
                                        ),
                                ),
                            ),
                        lahdejarjestelmanId = LahdejarjestelmanTunniste.randomFrom(Lahdejarjestelma.OPHTesti),
                    ),
            ),
        )
    }

    @GetMapping("/yki-arvioija.json", produces = ["application/json;charset=UTF-8"])
    fun ykiArvioija() =
        exampleJson(
            YkiArvioija(
                arvioijaOid = Oid.parse("1.2.246.562.24.59267607404").getOrThrow(),
                sukunimi = "Kivinen-Testi",
                etunimet = "Petro Testi",
                sahkopostiosoite = "devnull-2@oph.fi",
                katuosoite = "Haltin vanha autiotupa",
                postinumero = "99490",
                postitoimipaikka = "Enontekiö",
                ensimmainenRekisterointipaiva = LocalDate.of(2005, 1, 21),
                arviointioikeudet =
                    listOf(
                        YkiArviointioikeus(
                            kaudenAlkupaiva = LocalDate.of(2005, 12, 7),
                            kaudenPaattymispaiva = LocalDate.of(2020, 12, 7),
                            jatkorekisterointi = false,
                            tila = YkiArvioijaTila.AKTIIVINEN,
                            kieli = Tutkintokieli.FIN,
                            tasot = setOf(Tutkintotaso.PT, Tutkintotaso.KT, Tutkintotaso.YT),
                        ),
                        YkiArviointioikeus(
                            kaudenAlkupaiva = LocalDate.of(2006, 12, 7),
                            kaudenPaattymispaiva = LocalDate.of(2021, 12, 7),
                            jatkorekisterointi = false,
                            tila = YkiArvioijaTila.AKTIIVINEN,
                            kieli = Tutkintokieli.SWE,
                            tasot = setOf(Tutkintotaso.PT, Tutkintotaso.KT),
                        ),
                    ),
            ),
        )

    @GetMapping("/tiedonsiirto-ok.json", produces = ["application/json;charset=UTF-8"])
    fun tiedonsiirtoOkResponse() =
        exampleJson(
            TiedonsiirtoSuccess(),
        )

    @GetMapping("/bad-request-invalid-oid.json", produces = ["application/json;charset=UTF-8"])
    fun badRequestInvalidOidResponse() =
        exampleJson(
            TiedonsiirtoFailure.badRequest(
                "JSON parse error: Cannot construct instance of `fi.oph.kitu.Oid`, problem: Improperly formatted Object Identifier String - 123",
            ),
        )

    @GetMapping("/bad-request-arvioija-missing-value.json", produces = ["application/json;charset=UTF-8"])
    fun badRequestArvioijaResponse() =
        exampleJson(
            TiedonsiirtoFailure.badRequest(
                (
                    "JSON parse error: Instantiation of [simple type, class fi.oph.kitu.yki.arvioijat.YkiArvioija]" +
                        " value failed for JSON property arvioijaOid due to missing (therefore NULL) value for" +
                        " creator parameter arvioijaOid which is a non-nullable type"
                ),
            ),
        )

    @GetMapping("/tiedonsiirto-forbidden.json", produces = ["application/json;charset=UTF-8"])
    fun tiedonsiirtoForbiddenResponse() =
        exampleJson(
            TiedonsiirtoFailure.forbidden("Vain VKT-kielitutkinnon siirto sallittu"),
        )

    @GetMapping("/tiedonsiirto-forbidden-yki.json", produces = ["application/json;charset=UTF-8"])
    fun tiedonsiirtoForbiddenYkiResponse() =
        exampleJson(
            TiedonsiirtoFailure.forbidden("Vain YKI-kielitutkinnon siirto sallittu"),
        )

    @GetMapping("/yki-suoritus-ilmoittautunut.json", produces = ["application/json;charset=UTF-8"])
    fun ykiSuoritusIlmoittautunut() =
        exampleJson(
            ykiSuoritus(
                arviointitila = Arviointitila.ILMOITTAUTUNUT,
                arviointipaiva = null,
                arvosanat = listOf(null, null, null, null),
            ),
        )

    @GetMapping("/yki-suoritus-arvioitava.json", produces = ["application/json;charset=UTF-8"])
    fun ykiSuoritusArvioitava() =
        exampleJson(
            ykiSuoritus(
                arviointitila = Arviointitila.ARVIOITAVA,
                arviointipaiva = null,
                arvosanat = listOf(4, 5, null, null),
            ),
        )

    @GetMapping("/yki-suoritus-arvioitu.json", produces = ["application/json;charset=UTF-8"])
    fun ykiSuoritusArvioitu() =
        exampleJson(
            ykiSuoritus(
                arviointitila = Arviointitila.ARVIOITU,
                arviointipaiva = LocalDate.of(2026, 2, 3),
                arvosanat = listOf(4, 5, 4, 5),
            ),
        )

    @GetMapping("/yki-suoritus-ei-suoritusta.json", produces = ["application/json;charset=UTF-8"])
    fun ykiSuoritusEiSuoritusta() =
        exampleJson(
            ykiSuoritus(
                arviointitila = Arviointitila.EI_SUORITUSTA,
                arviointipaiva = null,
                arvosanat = listOf(9, 9, 10, 10),
            ),
        )

    @GetMapping("/yki-suoritus-tarkistusarvioitava.json", produces = ["application/json;charset=UTF-8"])
    fun ykiSuoritusTarkistusarvioitava() =
        exampleJson(
            ykiSuoritus(
                arviointitila = Arviointitila.TARKISTUSARVIOITAVA,
                arviointipaiva = LocalDate.of(2026, 2, 3),
                arvosanat = listOf(4, 5, 4, 5),
                tarkistusarviointi =
                    YkiTarkastusarviointi(
                        saapumispaiva = LocalDate.of(2026, 2, 13),
                        kasittelypaiva = null,
                        asiatunnus = "OPH-5000-1234",
                        tarkistusarvioidutOsakokeet = listOf(TutkinnonOsa.puhuminen),
                        arvosanaMuuttui = null,
                        perustelu = "",
                    ),
            ),
        )

    @GetMapping("/yki-suoritus.json", produces = ["application/json;charset=UTF-8"])
    fun ykiSuoritusTarkistusarvioitu() =
        exampleJson(
            ykiSuoritus(
                arviointitila = Arviointitila.TARKISTUSARVIOITU,
                arviointipaiva = LocalDate.of(2026, 2, 3),
                arvosanat = listOf(5, 5, 5, 5),
                tarkistusarviointi =
                    YkiTarkastusarviointi(
                        saapumispaiva = LocalDate.of(2026, 2, 13),
                        kasittelypaiva = LocalDate.of(2026, 2, 13),
                        asiatunnus = "OPH-5000-1234",
                        tarkistusarvioidutOsakokeet = listOf(TutkinnonOsa.puhuminen),
                        arvosanaMuuttui = listOf(TutkinnonOsa.puhuminen),
                        perustelu =
                            "Suorituksesta jäänyt viimeinen tehtävä arvioimatta. Arvioinnin jälkeen puhumisen taitotasoa 6.",
                    ),
            ),
        )

    private fun ykiSuoritus(
        arviointitila: Arviointitila,
        arviointipaiva: LocalDate?,
        arvosanat: List<Int?>,
        tarkistusarviointi: YkiTarkastusarviointi? = null,
    ) = Henkilosuoritus(
        henkilo =
            Henkilo(
                oid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                etunimet = "Ranja Testi",
                sukunimi = "Öhman-Testi",
                sukupuoli = Sukupuoli.N,
                kansalaisuus = "EST",
                katuosoite = "Testikuja 5",
                postinumero = "40100",
                postitoimipaikka = "Testilä",
                maa = "FIN",
                email = "testi@testi.fi",
            ),
        suoritus =
            YkiSuoritus(
                tutkintotaso = Tutkintotaso.YT,
                kieli = Tutkintokieli.FIN,
                todistuskieli = Todistuskieli.FIN,
                jarjestaja =
                    YkiJarjestaja(
                        oid = Oid.parse("1.2.246.562.10.14893989377").getOrThrow(),
                        nimi = "Jyväskylän yliopisto, Soveltavan kielentutkimuksen keskus",
                    ),
                tutkintopaiva = LocalDate.of(2026, 2, 2),
                arviointipaiva = arviointipaiva,
                arviointitila = arviointitila,
                osat =
                    listOf(
                        TutkinnonOsa.puhuminen,
                        TutkinnonOsa.puheenYmmartaminen,
                        TutkinnonOsa.kirjoittaminen,
                        TutkinnonOsa.tekstinYmmartaminen,
                    ).zip(arvosanat) { tyyppi, arvosana -> YkiOsa(tyyppi = tyyppi, arvosana = arvosana) },
                tarkistusarviointi = tarkistusarviointi,
                lahdejarjestelmanId = LahdejarjestelmanTunniste.randomFrom(Lahdejarjestelma.OPHTesti),
            ),
    )

    private fun exampleJson(data: Any): ResponseEntity<ByteArray> =
        ResponseEntity(objectMapper.writeValueAsBytes(data), HttpStatus.OK)

    private val objectMapper: ObjectMapper =
        defaultObjectMapper
            .rebuild()
            .annotationIntrospector(SchemaHiddenIntrospector())
            .build()
}

class SchemaHiddenIntrospector : JacksonAnnotationIntrospector() {
    override fun hasIgnoreMarker(
        config: MapperConfig<*>,
        m: AnnotatedMember,
    ): Boolean {
        val schema = m.getAnnotation(Schema::class.java)
        if (schema != null && schema.hidden) {
            return true
        }
        return super.hasIgnoreMarker(config, m)
    }
}
