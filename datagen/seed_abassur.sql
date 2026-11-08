-- ============================================================
-- AbAssurance : 1000 clients + 1500 contrats + 300 sinistres
--               + 1200 paiements synthetiques (demo soutenance)
-- Les clients g=1..100 ont un email partage avec AssurePlus (doublons).
-- ============================================================

INSERT INTO ab.AB_CLIENT (AB_CLIENT_ID, AB_NOM, AB_PRENOM, AB_DATE_NAISSANCE, AB_EMAIL, AB_TELEPHONE, AB_ADRESSE, AB_CODE_POSTAL, AB_NUM_FISCAL, AB_STATUT_CLIENT)
SELECT
  g + 100,
  (ARRAY['MARTIN','BERNARD','DUBOIS','THOMAS','ROBERT','RICHARD','PETIT','DURAND','LEFEBVRE','MOREAU'])[1 + (g % 10)],
  (ARRAY['Jean','Marie','Pierre','Sophie','Luc','Emma','Paul','Julie','Louis','Clara'])[1 + ((g * 7) % 10)],
  DATE '1955-01-01' + (g % 16000),
  CASE WHEN g <= 100
       THEN 'commun.client' || g || '@cross-systems.fr'
       ELSE 'ab.client' || g || '@demo.fr' END,
  '06' || lpad(((g * 123457) % 99999999)::text, 8, '0'),
  (g % 200) || ' rue de la Republique',
  (ARRAY['75001','59000','69001','31000','33000','13001','44000','06000','76000','34000','67000','35000'])[1 + (g % 12)],
  'FISC' || lpad(g::text, 8, '0'),
  (ARRAY['ACTIF','ACTIF','ACTIF','ACTIF','ACTIF','SUSPENDU','INACTIF','RESILIE'])[1 + (g % 8)]
FROM generate_series(1, 1000) AS g
ON CONFLICT (AB_CLIENT_ID) DO NOTHING;

INSERT INTO ab.AB_CONTRAT (AB_POLICY_NUMBER, AB_CLIENT_ID, AB_TYPE_ASSURANCE, AB_DATE_DEBUT, AB_DATE_FIN, AB_PRIME_ANNUELLE, AB_STATUT_CONTRAT, AB_AGENCE_ID)
SELECT
  'AB-P' || lpad(g::text, 5, '0'),
  101 + (g % 1000),
  (ARRAY['AUTO','HABITATION','SANTE','VIE','PREVOYANCE'])[1 + (g % 5)],
  DATE '2020-01-01' + (g % 1800),
  NULL,
  200 + (g % 2800),
  (ARRAY['ACTIF','ACTIF','ACTIF','RESILIE','SUSPENDU'])[1 + (g % 5)],
  100 + (g % 20)
FROM generate_series(1, 1500) AS g
ON CONFLICT (AB_POLICY_NUMBER) DO NOTHING;

INSERT INTO ab.AB_SINISTRE (AB_CLAIM_ID, AB_POLICY_NUMBER, AB_DATE_SINISTRE, AB_MONTANT_ESTIME, AB_STATUT_SINISTRE, AB_DESCRIPTION)
SELECT
  g + 10000,
  'AB-P' || lpad(g::text, 5, '0'),
  TIMESTAMP '2023-01-01' + (g % 900) * INTERVAL '1 day' + (g % 24) * INTERVAL '1 hour',
  100 + (g % 14900),
  (ARRAY['REGLE','REGLE','EN_COURS','REJETE'])[1 + (g % 4)],
  'Sinistre demonstration #' || g
FROM generate_series(1, 300) AS g
ON CONFLICT (AB_CLAIM_ID) DO NOTHING;

INSERT INTO ab.AB_PAIEMENT (AB_PAYMENT_ID, AB_POLICY_NUMBER, AB_DATE_PAIEMENT, AB_MONTANT, AB_MODE_PAIEMENT)
SELECT
  g + 10000,
  'AB-P' || lpad(g::text, 5, '0'),
  DATE '2024-01-01' + (g % 700),
  100 + (g % 2900),
  (ARRAY['VIREMENT','PRELEVEMENT','CARTE','CHEQUE'])[1 + (g % 4)]
FROM generate_series(1, 1200) AS g
ON CONFLICT (AB_PAYMENT_ID) DO NOTHING;
