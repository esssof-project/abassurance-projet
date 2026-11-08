-- ============================================================
-- AssurePlus : 1000 users + 1500 contrats + 300 sinistres
--               + 1200 paiements synthetiques (demo soutenance)
-- Les users g=1..100 ont le meme email que les clients AbAssurance.
-- ============================================================

INSERT INTO ap.AP_USERS (AP_USER_ID, AP_FULL_NAME, AP_BIRTH_DATE, AP_MAIL_ADDRESS, AP_PHONE_NUMBER, AP_STREET_ADDRESS, AP_ZIP_CODE, AP_CREATED_AT, AP_CUSTOMER_STATUS, AP_LOYALTY_SCORE)
SELECT
  g + 100,
  (ARRAY['MARTIN','BERNARD','DUBOIS','THOMAS','ROBERT','RICHARD','PETIT','DURAND','LEFEBVRE','MOREAU'])[1 + ((g * 3) % 10)] || ' ' ||
  (ARRAY['Jean','Marie','Pierre','Sophie','Luc','Emma','Paul','Julie','Louis','Clara'])[1 + ((g * 7) % 10)],
  TO_CHAR(DATE '1955-01-01' + (g % 16000), 'YYYY-MM-DD'),
  CASE WHEN g <= 100
       THEN 'commun.client' || g || '@cross-systems.fr'
       ELSE 'ap.user' || g || '@demo.fr' END,
  '07' || lpad(((g::bigint * 7654321) % 99999999)::text, 8, '0'),
  (g % 150) || ' avenue des Demo',
  (ARRAY['75001','59000','69001','31000','33000','13001','44000','06000','76000','34000','67000','35000'])[1 + ((g * 5) % 12)],
  TIMESTAMP '2023-01-15' + (g % 500) * INTERVAL '1 day',
  (ARRAY['ACTIF','ACTIF','ACTIF','ACTIF','SUSPENDU','INACTIF'])[1 + (g % 6)],
  g % 101
FROM generate_series(1, 1000) AS g
ON CONFLICT (AP_USER_ID) DO NOTHING;

INSERT INTO ap.AP_CONTRACTS (AP_CONTRACT_REF, AP_USER_ID, AP_PRODUCT_CODE, AP_START_DATE, AP_END_DATE, AP_MONTHLY_PREMIUM, AP_CONTRACT_STATE, AP_BROKER_CODE)
SELECT
  'AP-C' || lpad(g::text, 5, '0'),
  101 + (g % 1000),
  (ARRAY['AUTO','HABITATION','SANTE','VIE','PREVOYANCE'])[1 + ((g * 2) % 5)],
  TIMESTAMP '2020-01-01' + (g % 1500) * INTERVAL '1 day',
  NULL,
  20 + (g % 280),
  (ARRAY['ACTIF','ACTIF','ACTIF','RESILIE','SUSPENDU'])[1 + (g % 5)],
  'BKR' || lpad((g % 50)::text, 2, '0')
FROM generate_series(1, 1500) AS g
ON CONFLICT (AP_CONTRACT_REF) DO NOTHING;

INSERT INTO ap.AP_CLAIMS (AP_SINISTRE_NUM, AP_CONTRACT_REF, AP_INCIDENT_DATE, AP_ESTIMATED_AMOUNT, AP_CLAIM_STATUS, AP_CLAIM_COMMENT, AP_FRAUD_SCORE)
SELECT
  g + 10000,
  'AP-C' || lpad(g::text, 5, '0'),
  TO_CHAR(TIMESTAMP '2023-01-01' + (g % 900) * INTERVAL '1 day', 'YYYY-MM-DD HH24:MI:SS'),
  100 + (g % 9900),
  (ARRAY['REGLE','REGLE','EN_COURS','REJETE'])[1 + (g % 4)],
  'Sinistre demo #' || g,
  ROUND((g % 500) / 100.0, 2)
FROM generate_series(1, 300) AS g
ON CONFLICT (AP_SINISTRE_NUM) DO NOTHING;

INSERT INTO ap.AP_PAYMENTS (AP_PAYMENT_REF, AP_CONTRACT_REF, AP_PAYMENT_DATETIME, AP_AMOUNT_PAID, AP_PAYMENT_CHANNEL, AP_TRANSACTION_STATUS)
SELECT
  ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
  'AP-C' || lpad(g::text, 5, '0'),
  TIMESTAMP '2024-01-01' + (g % 700) * INTERVAL '1 day',
  100 + (g % 2900),
  (ARRAY['VIREMENT','CARTE','PRELEVEMENT'])[1 + (g % 3)],
  'VALIDE'
FROM generate_series(1, 1200) AS g
ON CONFLICT (AP_PAYMENT_REF) DO NOTHING;
