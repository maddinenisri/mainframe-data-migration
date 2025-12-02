      *****************************************************************
      *    Data-structure for Transaction Category Balance (RECLN = 50)
      *    Derived from CBACT04C.cbl FD-TRAN-CAT-BAL-RECORD structure
      *****************************************************************
       01  TRAN-CAT-BAL-RECORD.
           05  TRANCAT-ACCT-ID                  PIC 9(11).
           05  TRANCAT-TYPE-CD                  PIC X(02).
           05  TRANCAT-CD                       PIC 9(04).
           05  TRANCAT-BAL-DATA                 PIC X(33).
      *
      * Generated for CardDemo mainframe migration
      *
