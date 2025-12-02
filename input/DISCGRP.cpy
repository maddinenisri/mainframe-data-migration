      *****************************************************************
      *    Data-structure for Discount Group record (RECLN = 50)
      *    Derived from CBACT04C.cbl FD-DISCGRP-REC structure
      *****************************************************************
       01  DISCGRP-RECORD.
           05  DIS-ACCT-GROUP-ID                PIC X(10).
           05  DIS-TRAN-TYPE-CD                 PIC X(02).
           05  DIS-TRAN-CAT-CD                  PIC 9(04).
           05  DIS-DISCOUNT-RATE                PIC X(34).
      *
      * Generated for CardDemo mainframe migration
      *
