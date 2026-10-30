import sys
from xml.sax.saxutils import escape

W = 1600
CHAR = 6.3
LINE = 14

STYLE = """
    text { font-family: Helvetica, Arial, sans-serif; fill: #1f2937; }
    .tausta { fill: #ffffff; }
    .otsikko { font-size: 24px; font-weight: bold; }
    .alaotsikko { font-size: 13px; fill: #4b5563; }
    .kaista { fill: #f8fafc; stroke: #e2e8f0; stroke-width: 1; }
    .kaistan-nimi { font-size: 12px; font-weight: bold; fill: #64748b; letter-spacing: 0.5px; }
    .laatikko rect { stroke-width: 1.5; }
    .nimi { font-size: 14px; font-weight: bold; }
    .lisatieto { font-size: 11px; fill: #4b5563; }
    .kitu rect { fill: #dbeafe; stroke: #1e40af; stroke-width: 3; }
    .kios rect { fill: #ede9fe; stroke: #6d28d9; stroke-width: 2.5; }
    .oph rect { fill: #eff6ff; stroke: #60a5fa; }
    .ulkoinen rect { fill: #fff7ed; stroke: #ea580c; }
    .infra rect { fill: #f0fdf4; stroke: #16a34a; }
    .toimija rect { fill: #f3f4f6; stroke: #6b7280; }
    .ryhma { fill: #f5f9ff; stroke: #60a5fa; stroke-width: 1.5; stroke-dasharray: 4 3; }
    .ryhman-nimi { font-size: 14px; font-weight: bold; fill: #1e3a8a; }
    .nuoli path { fill: none; stroke: #374151; stroke-width: 1.6; }
    .heti path { stroke: #0e7490; stroke-width: 2.2; }
    .ajastettu path { stroke: #a16207; stroke-width: 2.2; }
    .pull path { stroke-dasharray: 7 4; }
    .kayttaja path { stroke: #9ca3af; stroke-width: 1.4; }
    .nuolen-teksti rect { fill: #ffffff; fill-opacity: 0.92; }
    .nuolen-teksti text { font-size: 11.5px; }
    .ei-kaytossa { opacity: 0.5; }
    .selite { font-size: 11.5px; }
"""


NUOLITYYPIT = (
    ("heti", "push heti"),
    ("ajastettu", "push ajastetusti"),
    ("push", "push, ajoitus ei tiedossa"),
    ("pull", "haku (pull)"),
    ("kayttaja", "käyttäjän toiminto"),
)
KARKIVARIT = {
    "heti": "#0e7490",
    "ajastettu": "#a16207",
    "push": "#374151",
    "pull": "#374151",
    "kayttaja": "#9ca3af",
}


def tw(s, size=11.5):
    return len(s) * CHAR * size / 12


class Svg:
    def __init__(self, height, title, subtitle, width=W):
        self.w = width
        self.h = height
        self.out = []
        self.title = title
        self.subtitle = subtitle

    def add(self, s, indent=1):
        self.out.append("  " * indent + s)

    def lane(self, id_, x, y, w, h, name):
        self.add(f'<g id="kaista-{id_}">')
        self.add(f'<rect class="kaista" x="{x}" y="{y}" width="{w}" height="{h}" rx="10"/>', 2)
        self.add(f'<text class="kaistan-nimi" x="{x + 14}" y="{y + 20}">{escape(name.upper())}</text>', 2)
        self.add("</g>")

    def box(self, id_, cls, x, y, w, h, name, lines=(), extra_cls=""):
        rx = 28 if cls == "toimija" else 8
        c = f"laatikko {cls} {extra_cls}".strip()
        self.add(f'<g id="{id_}" class="{c}">')
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}"/>', 2)
        n = 1 + len(lines)
        total = 18 + (n - 1) * 15
        cy = y + h / 2 - total / 2 + 13
        cx = x + w / 2
        self.add(f'<text class="nimi" x="{cx:g}" y="{cy:g}" text-anchor="middle">{escape(name)}</text>', 2)
        for i, ln in enumerate(lines):
            self.add(
                f'<text class="lisatieto" x="{cx:g}" y="{cy + 17 + i * 15:g}" text-anchor="middle">{escape(ln)}</text>',
                2,
            )
        self.add("</g>")

    def group(self, id_, x, y, w, h, name):
        self.add(f'<g id="{id_}">')
        self.add(f'<rect class="ryhma" x="{x}" y="{y}" width="{w}" height="{h}" rx="12"/>', 2)
        self.add(f'<text class="ryhman-nimi" x="{x + 14}" y="{y + 22}">{escape(name)}</text>', 2)
        self.add("</g>")

    def info_box(self, id_, cls, x, y, w, h, name, lines):
        self.add(f'<g id="{id_}" class="laatikko {cls}">')
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8"/>', 2)
        self.add(f'<text class="nimi" x="{x + 14}" y="{y + 24}">{escape(name)}</text>', 2)
        for i, ln in enumerate(lines):
            self.add(f'<text class="lisatieto" x="{x + 14}" y="{y + 46 + i * 16}">{escape(ln)}</text>', 2)
        self.add("</g>")

    def arrow(self, id_, pts, label, lx, ly, anchor="middle", kind="push", both=False, extra_cls=""):
        d = "M " + " L ".join(f"{x:g},{y:g}" for x, y in pts)
        c = f"nuoli {kind} {extra_cls}".strip()
        self.add(f'<g id="{id_}" class="{c}">')
        start = f' marker-start="url(#karki-{kind})"' if both else ""
        self.add(f'<path d="{d}" marker-end="url(#karki-{kind})"{start}/>', 2)
        if label:
            width = max(tw(s) for s in label) + 10
            height = len(label) * LINE + 6
            if anchor == "middle":
                rx_ = lx - width / 2
            elif anchor == "end":
                rx_ = lx - width + 5
            else:
                rx_ = lx - 5
            top = ly - 11 - 3
            self.add('<g class="nuolen-teksti">', 2)
            self.add(f'<rect x="{rx_:.0f}" y="{top:.0f}" width="{width:.0f}" height="{height:.0f}" rx="3"/>', 3)
            self.add(f'<text x="{lx:g}" y="{ly:g}" text-anchor="{anchor}">', 3)
            for i, s in enumerate(label):
                self.add(f'<tspan x="{lx:g}" y="{ly + i * LINE:g}">{escape(s)}</tspan>', 4)
            self.add("</text>", 3)
            self.add("</g>", 2)
        self.add("</g>")

    def legend(self, items):
        self.add('<g id="selite">')
        x = 660
        for cls, name in items:
            self.add(f'<g class="laatikko {cls}">', 2)
            rx = 7 if cls == "toimija" else 3
            self.add(f'<rect x="{x}" y="14" width="22" height="14" rx="{rx}"/>', 3)
            self.add("</g>", 2)
            self.add(f'<text class="selite" x="{x + 28}" y="25">{escape(name)}</text>', 2)
            x += 28 + tw(name) + 22
        x = 660
        for kind, name in NUOLITYYPIT:
            self.add(f'<g class="nuoli {kind}">', 2)
            self.add(f'<path d="M {x},44 L {x + 30},44" marker-end="url(#karki-{kind})"/>', 3)
            self.add("</g>", 2)
            self.add(f'<text class="selite" x="{x + 36}" y="48">{escape(name)}</text>', 2)
            x += 36 + tw(name) + 18
        self.add(
            f'<text class="selite" x="{x}" y="48">nuoli = tiedon kulkusuunta</text>',
            2,
        )
        self.add("</g>")

    def render(self):
        head = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" viewBox="0 0 {self.w} {self.h}">',
            f"  <title>{escape(self.title)}</title>",
            "  <defs>",
            "    <style>" + STYLE + "    </style>",
            *[
                line
                for kind, color in KARKIVARIT.items()
                for line in (
                    f'    <marker id="karki-{kind}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" '
                    'markerHeight="8" orient="auto-start-reverse">',
                    f'      <path d="M 0,0 L 10,5 L 0,10 z" fill="{color}"/>',
                    "    </marker>",
                )
            ],
            "  </defs>",
            f'  <rect class="tausta" x="0" y="0" width="{self.w}" height="{self.h}"/>',
            '  <g id="otsikko">',
            f'    <text class="otsikko" x="24" y="34">{escape(self.title)}</text>',
            f'    <text class="alaotsikko" x="24" y="54">{escape(self.subtitle)}</text>',
            "  </g>",
        ]
        return "\n".join(head + self.out + ["</svg>", ""])


LEGEND_BASE = [
    ("kitu", "Kielitutkintorekisteri"),
    ("kios", "KIOS"),
    ("oph", "Muu OPH-palvelu"),
    ("ulkoinen", "Ulkoinen palvelu"),
    ("toimija", "Käyttäjä"),
]

PAIVAYS = "30.10.2026"
SUB = f"Kielitutkintorekisterin (kitu) ja KIOS-ilmoittautumisjärjestelmän integraatiot · tilanne {PAIVAYS}"


def shared(s, y, boxes):
    box_h = 44 + 16 * max(len(lines) for _, _, lines in boxes)
    s.lane("yhteiset", 20, y, s.w - 40, box_h + 48, "Yhteiset OPH-palvelut")
    s.h = y + box_h + 48 + 20
    n = len(boxes)
    gap = 16
    w = (s.w - 40 - 28 - gap * (n - 1)) / n
    for i, (id_, name, lines) in enumerate(boxes):
        s.info_box(id_, "oph", 34 + i * (w + gap), y + 34, round(w), box_h, name, lines)


def top_row(s, suomifi_lines=("cas-oppija",), viesti_lines=("sähköpostit",)):
    s.box("suomifi", "ulkoinen", 290, 100, 180, 56, "Suomi.fi-tunnistus", suomifi_lines)
    s.box("paytrail", "ulkoinen", 500, 100, 180, 56, "Paytrail", ["verkkomaksut"])
    s.box("viestinvalitys", "oph", 710, 100, 190, 56, "Viestinvälitys", viesti_lines)


def yki_suoritukset():
    s = Svg(1040, "YKI – suoritukset: tietovirrat", SUB, width=1900)
    s.legend(LEGEND_BASE)
    s.lane("toimijat", 20, 70, 230, 740, "Käyttäjät")
    s.lane("ilmoittautuminen", 270, 70, 650, 740, "Ilmoittautuminen ja arviointi")
    s.lane("rekisteri", 940, 70, 940, 740, "Rekisteri, todistukset ja tiedottaminen")

    s.box("kansalainen", "toimija", 50, 120, 170, 56, "Kansalainen", ["tutkinnon suorittaja"])
    s.box("jarjestaja", "toimija", 50, 280, 170, 56, "Tutkinnon järjestäjä", ["virkailija-CAS"])
    s.box("virkailija", "toimija", 50, 430, 170, 56, "OPH-virkailija", ["virkailija-CAS"])
    top_row(s)
    s.box("kios", "kios", 430, 300, 280, 90, "KIOS", ["YKI-ilmoittautumisjärjestelmä", "yki.opintopolku.fi"])
    s.box("solki", "ulkoinen", 430, 580, 280, 130, "Solki", ["Jyväskylän yliopisto", "arvioinnit"])
    s.box("kitu", "kitu", 1040, 570, 300, 140, "Kielitutkintorekisteri", ["kitu", "YKI-suoritukset"])
    s.box(
        "oma-opintopolku",
        "oph",
        1020,
        100,
        480,
        50,
        "Oma Opintopolku",
        ["kansalainen näkee suoritukset ja lataa todistuksen"],
    )
    s.group("koski", 1000, 175, 520, 285, "KOSKI")
    s.box("koski-opiskeluoikeudet", "oph", 1020, 220, 200, 60, "Opiskeluoikeudet", ["suoritusten vastaanotto"])
    s.box("koski-todistuspalvelu", "oph", 1300, 220, 200, 70, "Todistuspalvelu", ["PDF-todistus", "sähköinen allekirjoitus"])
    s.box("koski-tiedotteet", "oph", 1300, 360, 200, 70, "Tiedotteiden lähetys", ["tunneittain,", "vahvistuksesta 24 h"])
    s.box("swisscom", "ulkoinen", 1640, 225, 220, 60, "Swisscom AIS", ["sähköinen allekirjoitus"])
    s.box("tiedotuspalvelu", "oph", 1640, 360, 220, 80, "Tiedotuspalvelu", ["omat-viestit", "ONR-tiimin palvelu"])
    s.box("suomifi-viestit", "ulkoinen", 1640, 520, 220, 70, "Suomi.fi-viestit", ["sähköinen viesti tai", "paperikirje (Posti)"])

    s.arrow("kansalainen-kios", [(220, 160), (430, 322)], ["ilmoittautuminen,", "tarkistusarvioinnin tilaus"], 268, 206, kind="kayttaja")
    s.arrow("suomifi-kios", [(380, 156), (470, 300)], ["tunnistautuminen", "tai sähköpostilinkki"], 428, 222, kind="heti")
    s.arrow("kios-paytrail", [(590, 300), (590, 156)], ["maksut"], 590, 232, both=True, kind="heti")
    s.arrow("kios-viestinvalitys", [(680, 300), (790, 156)], ["vahvistukset"], 735, 232, kind="push")
    s.arrow("jarjestaja-kios", [(220, 308), (430, 336)], ["tutkintotilaisuudet,", "ilmoittautuneet"], 325, 348, kind="kayttaja")
    s.arrow("virkailija-kios", [(220, 458), (430, 378)], ["tutkintopäivät,", "järjestäjärekisteri"], 300, 398, kind="kayttaja")
    s.arrow(
        "kios-solki",
        [(520, 390), (520, 580)],
        ["järjestäjät,", "tutkintotilaisuudet,", "osallistujat (CSV),", "henkilömuutokset", "Basic · 59 min välein"],
        512,
        462,
        anchor="end",
        kind="ajastettu",
    )
    s.arrow(
        "solki-hakee-kiosista",
        [(640, 390), (640, 580)],
        ["järjestäjien tiedot", "Solki hakee · CAS"],
        650,
        480,
        anchor="start",
        kind="pull",
    )
    s.arrow(
        "solki-kitu-suoritukset",
        [(710, 645), (1040, 645)],
        ["suoritukset ja tarkistusarvioinnit", "POST /yki/api/suoritus · OAuth2"],
        875,
        620,
        kind="push",
    )
    s.arrow(
        "kitu-kios-arviointitila",
        [(1040, 585), (710, 365)],
        ["arviointitila", "POST /yki/v2/api/oauth2/", "registration/evaluation", "OAuth2 · heti + tunneittain"],
        895,
        448,
        kind="heti",
    )
    s.arrow(
        "kitu-koski",
        [(1080, 570), (1080, 280)],
        ["suoritukset", "PUT /koski/api/oppija", "Basic · 15 min välein"],
        1072,
        392,
        anchor="end",
        kind="ajastettu",
    )
    s.arrow(
        "koski-hakee-yhteystiedot",
        [(1320, 570), (1320, 430)],
        ["yhteystiedot ja todistuskieli", "KOSKI hakee · OAuth2"],
        1330,
        500,
        anchor="start",
        kind="pull",
    )
    s.arrow("opiskeluoikeudet-oma-opintopolku", [(1120, 220), (1120, 150)], ["suoritukset"], 1120, 196, kind="pull")
    s.arrow("todistuspalvelu-oma-opintopolku", [(1400, 220), (1400, 150)], ["allekirjoitettu PDF"], 1400, 196, kind="pull")
    s.arrow("opiskeluoikeudet-todistuspalvelu", [(1220, 250), (1300, 250)], ["vahvistettu", "suoritus"], 1260, 226, kind="push")
    s.arrow("todistuspalvelu-tiedotteet", [(1400, 290), (1400, 360)], ["tulostettava PDF"], 1410, 329, anchor="start", kind="ajastettu")
    s.arrow("todistuspalvelu-swisscom", [(1500, 255), (1640, 255)], ["allekirjoitus"], 1570, 240, both=True, kind="heti")
    s.arrow(
        "tiedotteet-tiedotuspalvelu",
        [(1500, 395), (1640, 395)],
        ["tiedote ja PDF (S3)", "POST /omat-viestit/…", "OAuth2"],
        1570,
        414,
        kind="ajastettu",
    )
    s.arrow("tiedotuspalvelu-suomifi-viestit", [(1750, 440), (1750, 520)], ["viesti ja PDF"], 1760, 484, anchor="start", kind="push")
    s.arrow(
        "kios-hakee-koskesta",
        [(1020, 255), (710, 330)],
        ["koulutustiedot maksuttomuuden", "perusteeksi · KIOS hakee · Basic"],
        865,
        272,
        kind="pull",
    )
    s.arrow(
        "virkailija-kitu",
        [(135, 486), (135, 780), (1190, 780), (1190, 710)],
        ["tarkistusarviointien hyväksyntä, CSV-lataukset · virkailija-CAS"],
        660,
        774,
        kind="kayttaja",
    )
    shared(
        s,
        830,
        [
            (
                "onr",
                "Oppijanumerorekisteri",
                [
                    "KIOS: henkilöiden haku ja luonti",
                    "kitu: henkilötiedot ja master-OID (haku)",
                    "Tiedotuspalvelu: vastaanottajan henkilötiedot",
                ],
            ),
            ("koodisto", "Koodistopalvelu", ["KIOS: maakoodit Solkiin", "kitu: koodistot validointiin"]),
            (
                "organisaatiopalvelu",
                "Organisaatiopalvelu",
                ["KIOS: järjestäjien tiedot (myös Solkiin)", "kitu: organisaatiot validointiin"],
            ),
            (
                "otuva",
                "Otuva",
                ["virkailijoiden CAS-kirjautuminen", "OAuth2-tokenit integraatioille", "(myös KOSKI → kitu ja Tiedotuspalvelu)"],
            ),
        ],
    )
    return s


def yki_arvioijat():
    s = Svg(
        860,
        "YKI – arvioijarekisteri: tietovirrat",
        f"Kielitutkintorekisteri (kitu) on YKI-arvioijarekisterin master · tilanne {PAIVAYS}",
    )
    s.legend([("kitu", "Kielitutkintorekisteri"), ("oph", "Muu OPH-palvelu"), ("ulkoinen", "Ulkoinen palvelu"), ("toimija", "Käyttäjä")])
    s.lane("toimijat", 20, 70, 230, 570, "Käyttäjät")
    s.lane("hakemus", 270, 70, 650, 570, "Hakemus ja arviointi")
    s.lane("rekisteri", 940, 70, 640, 570, "Rekisteri")

    s.box("arvioija", "toimija", 50, 130, 170, 56, "Arvioija", ["hakija"])
    s.box("virkailija", "toimija", 50, 400, 170, 56, "OPH-virkailija", ["virkailija-CAS"])
    s.box(
        "ataru",
        "oph",
        430,
        120,
        280,
        76,
        "Ataru",
        ["arvioijahakemuslomake", "integraatio ei vielä käytössä"],
        "ei-kaytossa",
    )
    s.box(
        "solki",
        "ulkoinen",
        430,
        330,
        280,
        130,
        "Solki",
        ["Jyväskylän yliopisto", "arvioijat arviointityössä"],
    )
    s.box(
        "kitu",
        "kitu",
        1040,
        320,
        300,
        150,
        "Kielitutkintorekisteri",
        ["kitu", "arvioijat, arviointikaudet", "ja arviointioikeudet"],
    )

    s.arrow("arvioija-ataru", [(220, 158), (430, 158)], ["arvioijahakemus"], 325, 150, kind="kayttaja")
    s.arrow(
        "kitu-hakee-atarusta",
        [(710, 170), (1100, 320)],
        ["arvioijahakemukset → uusi arvioija tai jatkokausi", "kitu hakee · CAS · öisin · ei vielä käytössä"],
        905,
        232,
        extra_cls="ei-kaytossa",
        kind="pull",
    )
    s.arrow(
        "solki-kitu-yhteystiedot",
        [(710, 360), (1040, 360)],
        ["arvioijien yhteystiedot", "POST /yki/api/arvioija · OAuth2"],
        875,
        335,
        kind="push",
    )
    s.arrow(
        "kitu-solki-arvioijarekisteri",
        [(1040, 430), (710, 430)],
        ["arvioijarekisteri: kaudet ja kielet", "POST /oph/arvioija · Basic · tallennuksesta", "vastauksena Solkin arvioijatunnus"],
        875,
        391,
        kind="heti",
    )
    s.arrow(
        "virkailija-kitu",
        [(135, 456), (135, 560), (1190, 560), (1190, 470)],
        ["arvioijat, kaudet, passivointi ja käsin lähetys Solkiin · virkailija-CAS"],
        660,
        554,
        kind="kayttaja",
    )
    shared(
        s,
        660,
        [
            (
                "onr",
                "Oppijanumerorekisteri",
                ["kitu: arvioijan nimet ja yksilöinti,", "syntymäaika Solki-lähetykseen (ei tallenneta)"],
            ),
            (
                "otuva",
                "Otuva",
                ["virkailijoiden CAS-kirjautuminen", "OAuth2-token Solkille, CAS-tunnus Ataruun"],
            ),
        ],
    )
    return s


def vkt_et():
    s = Svg(964, "VKT erinomainen taitotaso: tietovirrat", SUB)
    s.legend(LEGEND_BASE + [("infra", "AWS-infra")])
    s.lane("toimijat", 20, 70, 230, 680, "Käyttäjät")
    s.lane("ilmoittautuminen", 270, 70, 650, 680, "Ilmoittautuminen")
    s.lane("rekisteri", 940, 70, 640, 680, "Arviointi, rekisteri ja julkaisu")

    s.box("kansalainen", "toimija", 50, 120, 170, 56, "Kansalainen", ["tutkinnon suorittaja"])
    s.box("virkailija", "toimija", 50, 290, 170, 56, "OPH-virkailija", ["KIOS ja kitu · virkailija-CAS"])
    top_row(s, viesti_lines=("vahvistukset, kuitit",))
    s.box("kios", "kios", 430, 300, 280, 90, "KIOS", ["VKT-ilmoittautumisjärjestelmä", "OPH:n tutkintotilaisuudet"])
    s.box("s3", "infra", 470, 560, 200, 56, "S3", ["liitteet"])
    s.box("kitu", "kitu", 1040, 320, 300, 120, "Kielitutkintorekisteri", ["kitu", "arvosanojen kirjaus"])
    s.box("koski", "oph", 1040, 140, 200, 60, "KOSKI")
    s.box("oma-opintopolku", "oph", 1360, 140, 190, 60, "Oma Opintopolku", ["kansalainen näkee suorituksen"])

    s.arrow(
        "kansalainen-kios",
        [(220, 160), (430, 322)],
        ["paikan varaus tai jonoon,", "osakokeiden valinta"],
        268,
        206,
        kind="kayttaja",
    )
    s.arrow("suomifi-kios", [(380, 156), (470, 300)], ["tunnistautuminen"], 428, 228, kind="heti")
    s.arrow("kios-paytrail", [(590, 300), (590, 156)], ["maksut"], 590, 232, both=True, kind="heti")
    s.arrow("kios-viestinvalitys", [(680, 300), (790, 156)], ["sähköpostit · jono 10 s"], 722, 232, kind="ajastettu")
    s.arrow(
        "virkailija-kios",
        [(220, 318), (430, 345)],
        ["tutkintotilaisuudet,", "maksuttomuuden hyväksyntä,", "maksulinkit jonosta"],
        310,
        362,
        kind="kayttaja",
    )
    s.arrow(
        "kios-s3",
        [(570, 390), (570, 560)],
        ["maksuttomuuden todisteet", "selain lataa suoraan", "(esiallekirjoitettu URL)"],
        580,
        462,
        anchor="start",
        both=True,
        kind="heti",
    )
    s.arrow(
        "kios-kitu",
        [(710, 360), (1040, 360)],
        ["ilmoittautuneet osakokeet,", "ilman arvosanoja", "PUT /api/vkt/kios · CAS · tunneittain"],
        875,
        322,
        kind="ajastettu",
    )
    s.arrow(
        "kios-hakee-koskesta",
        [(1040, 168), (700, 300)],
        ["koulutustiedot maksuttomuuden", "perusteeksi · KIOS hakee · Basic"],
        900,
        226,
        kind="pull",
    )
    s.arrow(
        "virkailija-kitu",
        [(135, 346), (135, 700), (1190, 700), (1190, 440)],
        ["osakokeiden arvosanojen kirjaus · virkailija-CAS"],
        660,
        694,
        kind="kayttaja",
    )
    s.arrow(
        "kitu-koski",
        [(1140, 320), (1140, 200)],
        ["suoritukset, kun kaikki", "osakokeet on arvioitu", "PUT /koski/api/oppija · Basic · öisin"],
        1150,
        250,
        anchor="start",
        kind="ajastettu",
    )
    s.arrow("koski-oma-opintopolku", [(1240, 170), (1360, 170)], ["suoritukset"], 1300, 150, kind="pull")
    shared(
        s,
        770,
        [
            (
                "onr",
                "Oppijanumerorekisteri",
                ["KIOS: henkilöiden luonti ja nimitietojen päivitys", "kitu: master-OID (haku)"],
            ),
            ("koodisto", "Koodistopalvelu", ["kitu: koodistot validointiin"]),
            ("otuva", "Otuva", ["virkailijoiden CAS-kirjautuminen", "CAS-tiketti KIOS → kitu, ONR ja KOSKI"]),
        ],
    )
    return s


def vkt_htt():
    s = Svg(964, "VKT hyvä ja tyydyttävä taitotaso: tietovirrat", SUB)
    s.legend(LEGEND_BASE + [("infra", "AWS-infra")])
    s.lane("toimijat", 20, 70, 230, 680, "Käyttäjät")
    s.lane("ilmoittautuminen", 270, 70, 650, 680, "Ilmoittautuminen ja arviointi")
    s.lane("rekisteri", 940, 70, 640, 680, "Rekisteri ja julkaisu")

    s.box("kansalainen", "toimija", 50, 120, 170, 56, "Kansalainen", ["tutkinnon suorittaja"])
    s.box("vastaanottaja", "toimija", 50, 290, 170, 66, "Tutkinnon", ["vastaanottaja", "virkailija-CAS"])
    s.box("virkailija", "toimija", 50, 470, 170, 56, "OPH-virkailija", ["KIOS ja kitu · virkailija-CAS"])
    top_row(s, suomifi_lines=("cas-oppija · valinnainen",), viesti_lines=("linkit, vahvistukset",))
    s.box("kios", "kios", 430, 300, 280, 90, "KIOS", ["VKT-ilmoittautumisjärjestelmä", "vastaanottajien ajanvaraukset"])
    s.box("s3", "infra", 470, 560, 200, 56, "S3", ["liitteet"])
    s.box("kitu", "kitu", 1040, 320, 300, 120, "Kielitutkintorekisteri", ["kitu", "vain tarkastelu"])
    s.box("koski", "oph", 1040, 140, 200, 60, "KOSKI")
    s.box("oma-opintopolku", "oph", 1360, 140, 190, 60, "Oma Opintopolku", ["kansalainen näkee suorituksen"])

    s.arrow(
        "kansalainen-kios",
        [(220, 160), (430, 322)],
        ["yhteydenottopyyntö,", "ajanvarauksen täydennys"],
        268,
        206,
        kind="kayttaja",
    )
    s.arrow("suomifi-kios", [(380, 156), (470, 300)], ["tunnistautuminen"], 428, 228, kind="heti")
    s.arrow("kios-paytrail", [(590, 300), (590, 156)], ["maksut"], 590, 232, both=True, kind="heti")
    s.arrow("kios-viestinvalitys", [(680, 300), (790, 156)], ["sähköpostit · jono 10 s"], 722, 232, kind="ajastettu")
    s.arrow(
        "vastaanottaja-kios",
        [(220, 323), (430, 345)],
        ["arvioijaprofiili, tilaisuudet,", "ajanvaraukset ja arvosanat"],
        320,
        366,
        kind="kayttaja",
    )
    s.arrow(
        "virkailija-kios",
        [(220, 490), (450, 390)],
        ["tutkinnon vastaanottajien", "listaus"],
        330,
        446,
        kind="kayttaja",
    )
    s.arrow(
        "kios-s3",
        [(570, 390), (570, 560)],
        ["yhteydenottopyynnön liitteet", "selain lataa suoraan", "(esiallekirjoitettu URL)"],
        580,
        462,
        anchor="start",
        both=True,
        kind="heti",
    )
    s.arrow(
        "kios-kitu",
        [(710, 360), (1040, 360)],
        ["valmis suoritus arvosanoineen,", "vastaanottaja ja paikkakunta", "PUT /api/vkt/kios · CAS · tunneittain"],
        875,
        322,
        kind="ajastettu",
    )
    s.arrow(
        "kitu-virkailija",
        [(1190, 440), (1190, 700), (135, 700), (135, 526)],
        ["suoritusten tarkastelu (vain luku) · virkailija-CAS"],
        660,
        694,
        kind="kayttaja",
    )
    s.arrow(
        "kitu-koski",
        [(1140, 320), (1140, 200)],
        ["suoritukset", "PUT /koski/api/oppija · Basic · öisin"],
        1150,
        256,
        anchor="start",
        kind="ajastettu",
    )
    s.arrow("koski-oma-opintopolku", [(1240, 170), (1360, 170)], ["suoritukset"], 1300, 150, kind="pull")
    shared(
        s,
        770,
        [
            (
                "onr",
                "Oppijanumerorekisteri",
                ["KIOS: henkilön luonti hetusta tai syntymäajasta", "kitu: master-OID (haku)"],
            ),
            ("koodisto", "Koodistopalvelu", ["kitu: koodistot validointiin", "(KIOS käyttää omaa kuntalistaansa)"]),
            ("otuva", "Otuva", ["virkailijoiden CAS-kirjautuminen", "CAS-tiketti KIOS → kitu ja ONR"]),
        ],
    )
    return s


KOTO_LEGEND = [
    ("kitu", "Kielitutkintorekisteri"),
    ("oph", "Muu OPH-palvelu"),
    ("ulkoinen", "Ulkoinen palvelu"),
    ("infra", "AWS-infra"),
    ("toimija", "Käyttäjä"),
]


def koto_suoritukset():
    s = Svg(
        840,
        "Kotoutumiskoulutus – suoritukset: tietovirrat",
        f"Kielitutkintorekisterin (kitu) integraatiot kielitaidon päättötestissä · tilanne {PAIVAYS}",
    )
    s.legend([item for item in KOTO_LEGEND if item[0] != "infra"])
    s.lane("toimijat", 20, 70, 230, 480, "Käyttäjät")
    s.lane("kielitestialusta", 270, 70, 650, 480, "Kielitestialusta")
    s.lane("rekisteri", 940, 70, 640, 480, "Rekisteri")

    s.box("opiskelija", "toimija", 50, 140, 170, 56, "Opiskelija", ["kotoutumiskoulutus"])
    s.box("opettaja", "toimija", 50, 300, 170, 56, "Opettaja", ["Koealustan käyttäjä"])
    s.box("virkailija", "toimija", 50, 460, 170, 56, "OPH-virkailija", ["virkailija-CAS"])
    s.box(
        "koealusta",
        "ulkoinen",
        430,
        240,
        280,
        150,
        "Koealusta",
        ["Moodle · Mediamaisteri", "kielitesti.opintopolku.fi"],
    )
    s.box("kitu", "kitu", 1040, 240, 300, 150, "Kielitutkintorekisteri", ["kitu", "kielitestisuoritukset"])

    s.arrow("opiskelija-koealusta", [(220, 175), (430, 262)], ["päättötesti"], 325, 214, kind="kayttaja")
    s.arrow(
        "opettaja-koealusta",
        [(220, 328), (430, 328)],
        ["testien järjestäminen,", "nimien korjaus"],
        325,
        348,
        kind="kayttaja",
    )
    s.arrow(
        "virkailija-opettaja",
        [(135, 460), (135, 356)],
        ["korjauspyynnöt", "virheellisistä nimistä", "(kitun ulkopuolella)"],
        145,
        396,
        anchor="start",
        kind="kayttaja",
    )
    s.arrow(
        "kitu-hakee-valmiit",
        [(710, 295), (1040, 295)],
        ["valmiit suoritukset (inkrementaalinen)", "kitu hakee · wstoken · öisin"],
        875,
        270,
        kind="pull",
    )
    s.arrow(
        "kitu-hakee-keskeneraiset",
        [(710, 355), (1040, 355)],
        ["keskeneräiset (korvataan, 14 vrk ikkuna)", "kitu hakee · wstoken · öisin"],
        875,
        330,
        kind="pull",
    )
    s.arrow(
        "kitu-virkailija",
        [(1100, 390), (1100, 488), (220, 488)],
        ["suoritukset ja CSV-lataukset · virheiden näkymä nimineen ja korjausehdotuksineen", "/koto-kielitesti/suoritukset/virheet · virkailija-CAS"],
        660,
        474,
        kind="kayttaja",
    )
    shared(
        s,
        570,
        [
            ("onr", "Oppijanumerorekisteri", ["kitu: master-OID (haku)"]),
            ("organisaatiopalvelu", "Organisaatiopalvelu", ["kitu: organisaatioiden nimet"]),
            ("otuva", "Otuva", ["virkailijoiden CAS-kirjautuminen", "OAuth2-tokenit integraatioille"]),
        ],
    )
    return s


def koto_tehtavapankki():
    s = Svg(
        840,
        "Kotoutumiskoulutus – tehtäväpankki: tietovirrat",
        f"Koealustan kysymyspankkien tuonti kituun · tilanne {PAIVAYS}",
    )
    s.legend([item for item in KOTO_LEGEND if item[0] != "oph"])
    s.lane("toimijat", 20, 70, 230, 560, "Käyttäjät")
    s.lane("kielitestialusta", 270, 70, 650, 560, "Kielitestialusta")
    s.lane("rekisteri", 940, 70, 640, 560, "Rekisteri")

    s.box("virkailija", "toimija", 50, 420, 170, 56, "OPH-virkailija", ["virkailija-CAS"])
    s.box(
        "koealusta",
        "ulkoinen",
        430,
        240,
        280,
        150,
        "Koealusta",
        ["Moodle · Mediamaisteri", "kysymyspankit (Moodle-XML)"],
    )
    s.box(
        "kitu",
        "kitu",
        1040,
        240,
        300,
        150,
        "Kielitutkintorekisteri",
        ["kitu", "tehtäväpaketit, tehtävät", "ja vastaukset tietokannassa"],
    )
    s.box("s3", "infra", 1080, 480, 220, 70, "S3", ["XML-tiedostot,", "mp3- ja png-tiedostot"])

    s.arrow(
        "kitu-hakee-luettelon",
        [(710, 290), (1040, 290)],
        ["tehtäväpankkien luettelo", "export_question_bank · wstoken · klo 2"],
        875,
        265,
        kind="pull",
    )
    s.arrow(
        "kitu-hakee-xml",
        [(710, 350), (1040, 350)],
        ["Moodle-XML", "kitu hakee luettelon downloadurl-osoitteesta"],
        875,
        325,
        kind="pull",
    )
    s.arrow(
        "kitu-s3",
        [(1190, 390), (1190, 480)],
        ["XML ja mediatiedostot S3:een,", "luku tietokantaan · öisin"],
        1200,
        430,
        anchor="start",
        both=True,
        kind="ajastettu",
    )
    s.arrow(
        "kitu-virkailija",
        [(1080, 390), (1080, 448), (220, 448)],
        ["tehtäväpaketit ja tehtävät · virkailija-CAS"],
        640,
        442,
        kind="kayttaja",
    )
    s.arrow(
        "virkailija-hakee-s3",
        [(1190, 550), (1190, 600), (135, 600), (135, 476)],
        ["tiedostojen lataus: kitu ohjaa selaimen esiallekirjoitettuun S3-osoitteeseen"],
        660,
        594,
        kind="pull",
    )
    shared(
        s,
        650,
        [("otuva", "Otuva", ["virkailijoiden CAS-kirjautuminen"])],
    )
    return s


if __name__ == "__main__":
    outdir = sys.argv[1]
    for name, fn in (
        ("yki-suoritukset", yki_suoritukset),
        ("yki-arvioijat", yki_arvioijat),
        ("vkt-erinomainen", vkt_et),
        ("vkt-hyva-ja-tyydyttava", vkt_htt),
        ("kotoutumiskoulutus-suoritukset", koto_suoritukset),
        ("kotoutumiskoulutus-tehtavapankki", koto_tehtavapankki),
    ):
        with open(f"{outdir}/{name}.svg", "w", encoding="utf-8") as f:
            f.write(fn().render())
