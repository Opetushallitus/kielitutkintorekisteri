-- Taulun ainoa kirjoittaja oli Solkin poistettu CSV-tuonti. Lukeva koodi (nakyma
-- /yki/suoritukset/virheet, etusivun laskuri ja suorituslistan virheilmoitus) poistettiin
-- edellisessa julkaisussa, joten rinnakkain ajossa oleva vanha sovellusversio ei enaa
-- lue taulua eika paluu edelliseen versioon tarvitse sita.
DROP TABLE IF EXISTS yki_suoritus_error;
