ALTER TABLE yki_arvioijahakemus_tarkistettava
    ADD COLUMN maksun_tila     TEXT NOT NULL DEFAULT 'EI_TIEDOSSA'
        CHECK (maksun_tila IN ('MAKSETTU', 'AVOINNA', 'ERAANTYNYT', 'EI_LASKUA', 'EI_TIEDOSSA')),
    ADD COLUMN maksettu        DATE,
    ADD COLUMN maksun_erapaiva DATE;

COMMENT ON COLUMN yki_arvioijahakemus_tarkistettava.maksun_tila IS
    'Atarun ASTU-paatosmaksun tila haun hetkella (atarun reitti /api/maksut/list/{hakemus} -> maksut-palvelu). Vain virkailijalle naytettava tieto: hakemuksen ehto on atarun processed-tila, jonka atarun maksupolleri asettaa maksun jalkeen. Laskun maksulinkin salaisuutta tai maksajan nimia ei talleteta.';
