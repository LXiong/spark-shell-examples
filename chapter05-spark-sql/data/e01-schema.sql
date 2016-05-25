CREATE TABLE promotion__c
(
  date_from__c date,
  date_thru__c date,
  createddate timestamp without time zone,
  sfid character varying(18),
  slogan__c character varying(1300),
  anchor_account__c character varying(18),
  _hc_err text,
  slogan_language_1__c character varying(255),
  _hc_lastop character varying(32),
  name character varying(80),
  anchor_account__r__pkey__c character varying(22),
  systemmodstamp timestamp without time zone,
  id serial NOT NULL,
  isdeleted boolean,
  CONSTRAINT promotion__c_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE promotion_product
(
  promotion_sfid character varying(18) NOT NULL,
  product_sfid character varying(18) NOT NULL,
  CONSTRAINT promotion_product_pkey PRIMARY KEY (promotion_sfid, product_sfid)
)
WITH (
  OIDS=FALSE
);
