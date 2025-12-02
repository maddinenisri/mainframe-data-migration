-- PostgreSQL Schema for CardDemo DB2 Tables
-- Converted from DB2 for z/OS DDL
-- Migration Order: Parent tables first, then child tables

-- ============================================================================
-- Transaction Type (Parent Table)
-- ============================================================================
DROP TABLE IF EXISTS transaction_type_category CASCADE;
DROP TABLE IF EXISTS transaction_type CASCADE;

CREATE TABLE transaction_type (
    tr_type         CHAR(2)      NOT NULL,
    tr_description  VARCHAR(50)  NOT NULL,

    CONSTRAINT pk_transaction_type PRIMARY KEY (tr_type)
);

COMMENT ON TABLE transaction_type IS 'Transaction type reference data - migrated from DB2 CARDDEMO.TRANSACTION_TYPE';
COMMENT ON COLUMN transaction_type.tr_type IS 'Transaction type code (2 chars)';
COMMENT ON COLUMN transaction_type.tr_description IS 'Transaction type description';

-- ============================================================================
-- Transaction Type Category (Child Table)
-- ============================================================================
CREATE TABLE transaction_type_category (
    trc_type_code      CHAR(2)      NOT NULL,
    trc_type_category  CHAR(4)      NOT NULL,
    trc_cat_data       VARCHAR(50)  NOT NULL,

    CONSTRAINT pk_transaction_type_category
        PRIMARY KEY (trc_type_code, trc_type_category),

    CONSTRAINT fk_trc_type_code
        FOREIGN KEY (trc_type_code)
        REFERENCES transaction_type (tr_type)
        ON DELETE RESTRICT
);

COMMENT ON TABLE transaction_type_category IS 'Transaction category reference data - migrated from DB2 CARDDEMO.TRANSACTION_TYPE_CATEGORY';
COMMENT ON COLUMN transaction_type_category.trc_type_code IS 'Transaction type code (FK to transaction_type)';
COMMENT ON COLUMN transaction_type_category.trc_type_category IS 'Category code within type';
COMMENT ON COLUMN transaction_type_category.trc_cat_data IS 'Category description';

-- ============================================================================
-- Indexes (matching DB2 indexes)
-- ============================================================================
CREATE UNIQUE INDEX idx_tran_type
    ON transaction_type (tr_type ASC);

CREATE UNIQUE INDEX idx_tran_type_catg
    ON transaction_type_category (trc_type_code ASC, trc_type_category ASC);
