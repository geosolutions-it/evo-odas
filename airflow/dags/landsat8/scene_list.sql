CREATE TABLE scene_list (
    productid character varying(40) DEFAULT NULL::character varying,
    entityid character varying(21) DEFAULT NULL::character varying,
    acquisitiondate timestamp without time zone,
    cloudcover numeric(5,2) DEFAULT NULL::numeric,
    processinglevel character varying(4) DEFAULT NULL::character varying,
    path integer,
    "row" integer,
    min_lat numeric(8,5) DEFAULT NULL::numeric,
    min_lon numeric(9,5) DEFAULT NULL::numeric,
    max_lat numeric(8,5) DEFAULT NULL::numeric,
    max_lon numeric(9,5) DEFAULT NULL::numeric,
    download_url character varying(112) DEFAULT NULL::character varying
);
