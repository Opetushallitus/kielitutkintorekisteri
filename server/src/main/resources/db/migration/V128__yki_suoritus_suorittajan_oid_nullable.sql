-- Oppijanumero sallitaan puuttuvaksi, jotta YKI-historiamigraation (2011-2016) rivit, joille
-- oppijanumeroa ei saatu oppijanumerorekisteristä millään keinolla, voidaan siirtää rekisteriin
-- karanteenitaulusta yki_historia_siirtymaton. Siirron tekee V129.
--
-- NULL on vain historiadatan erikoistapaus eikä kelpaa uudelle tiedolle:
--   * rajapinta POST /yki/api/suoritus ei päästä läpi oppijanumerotonta suoritusta
--     (tiedontuontischema/Henkilo.oid on ei-nullable eikä kenttää voi jättää pois),
--   * suoritus ei siirry KOSKEen (KoskiYkiRequestMapper.koskiSiirronEstonSyyt) eikä
--     ilmoittautumisjärjestelmään (YkiArvioinninTila.of) ilman oppijanumeroa.
--
-- henkilo_oid-domainilla ei ole omaa NOT NULL -rajoitetta, joten sarakkeen rajoitteen
-- poistaminen riittää.
ALTER TABLE yki_suoritus
    ALTER COLUMN suorittajan_oid DROP NOT NULL;

COMMENT ON COLUMN yki_suoritus.suorittajan_oid IS
    'Suorittajahenkilön oppijanumero. NULL vain 2011-2016 historiadatalla, jolle oppijanumeroa ei saatu oppijanumerorekisteristä; tällainen suoritus ei siirry KOSKEen eikä ilmoittautumisjärjestelmään, eikä rajapinta hyväksy sitä.';
