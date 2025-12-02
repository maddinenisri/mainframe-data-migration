"""
EBCDIC Encoding Utilities.

Provides functions for converting between EBCDIC and ASCII/UTF-8 encodings.
Supports various mainframe code pages (CCSID).

Example:
    >>> converter = EBCDICConverter("cp037")
    >>> text = converter.decode(b"\\xc8\\x85\\x93\\x93\\x96")  # "Hello" in EBCDIC
    >>> print(text)
    Hello
"""

from typing import Optional
from decimal import Decimal


# CCSID (Coded Character Set Identifier) to Python codec mapping
CCSID_CODECS = {
    # US/English EBCDIC
    37: "cp037",
    # International EBCDIC
    500: "cp500",
    # Open Systems EBCDIC (Unix compatible)
    1047: "cp1047",
    # US EBCDIC with Euro
    1140: "cp1140",
    # Unicode encodings
    1200: "utf-16",
    1208: "utf-8",
    # Asian EBCDIC
    930: "cp930",     # Japanese mixed
    935: "cp935",     # Simplified Chinese
    937: "cp937",     # Traditional Chinese
    # European EBCDIC
    273: "cp273",     # German
    284: "cp284",     # Spanish
    285: "cp285",     # UK
    297: "cp297",     # French
}


class EBCDICConverter:
    """
    Converter for EBCDIC encoded data.

    This class provides utilities for converting EBCDIC data from
    mainframe systems to ASCII/UTF-8 for modern applications.

    Attributes:
        codec: Python codec name for the encoding
        ccsid: Original CCSID value

    Example:
        >>> converter = EBCDICConverter("cp037")
        >>> text = converter.decode(ebcdic_bytes)
        >>> ebcdic = converter.encode(text)
    """

    def __init__(self, codec: str = "cp037"):
        """
        Initialize the EBCDIC converter.

        Args:
            codec: Python codec name (e.g., 'cp037') or CCSID number as string
        """
        # Handle CCSID passed as string
        if codec.isdigit():
            ccsid = int(codec)
            codec = CCSID_CODECS.get(ccsid, "cp037")

        self.codec = codec
        self.ccsid = self._get_ccsid(codec)

    def _get_ccsid(self, codec: str) -> Optional[int]:
        """Get CCSID for a codec name."""
        for ccsid, name in CCSID_CODECS.items():
            if name == codec:
                return ccsid
        return None

    def decode(self, data: bytes, errors: str = "replace") -> str:
        """
        Decode EBCDIC bytes to string.

        Args:
            data: EBCDIC encoded bytes
            errors: Error handling ('strict', 'replace', 'ignore')

        Returns:
            Decoded string
        """
        if data is None:
            return ""
        return data.decode(self.codec, errors=errors)

    def encode(self, text: str, errors: str = "replace") -> bytes:
        """
        Encode string to EBCDIC bytes.

        Args:
            text: String to encode
            errors: Error handling ('strict', 'replace', 'ignore')

        Returns:
            EBCDIC encoded bytes
        """
        if text is None:
            return b""
        return text.encode(self.codec, errors=errors)

    def decode_packed_decimal(
        self,
        data: bytes,
        scale: int = 0,
    ) -> Decimal:
        """
        Decode packed decimal (COMP-3) to Decimal.

        Packed decimal format:
        - Each byte contains 2 digits (BCD)
        - Last nibble is the sign (C=positive, D=negative, F=unsigned)

        Args:
            data: Packed decimal bytes
            scale: Number of decimal places

        Returns:
            Python Decimal value

        Example:
            >>> converter.decode_packed_decimal(b'\\x12\\x34\\x5C', scale=2)
            Decimal('123.45')
        """
        if not data:
            return Decimal(0)

        # Extract digits
        digits = []
        for byte in data[:-1]:
            digits.append(byte >> 4)
            digits.append(byte & 0x0F)

        # Last byte: high nibble is digit, low nibble is sign
        digits.append(data[-1] >> 4)
        sign_nibble = data[-1] & 0x0F

        # Build number string
        num_str = "".join(str(d) for d in digits)

        # Insert decimal point
        if scale > 0:
            if len(num_str) <= scale:
                num_str = "0" * (scale - len(num_str) + 1) + num_str
            num_str = num_str[:-scale] + "." + num_str[-scale:]

        # Apply sign (D = negative)
        if sign_nibble == 0x0D:
            num_str = "-" + num_str

        return Decimal(num_str)

    def encode_packed_decimal(
        self,
        value: Decimal,
        precision: int,
        scale: int = 0,
    ) -> bytes:
        """
        Encode Decimal to packed decimal (COMP-3) bytes.

        Args:
            value: Decimal value to encode
            precision: Total number of digits
            scale: Number of decimal places

        Returns:
            Packed decimal bytes
        """
        # Determine sign
        sign = 0x0C if value >= 0 else 0x0D
        value = abs(value)

        # Scale to integer
        if scale > 0:
            value = value * (10 ** scale)
        value = int(value)

        # Convert to digit string, pad to precision
        digits = str(value).zfill(precision)

        # Pack digits into bytes
        result = bytearray()
        for i in range(0, len(digits) - 1, 2):
            high = int(digits[i])
            low = int(digits[i + 1]) if i + 1 < len(digits) else 0
            result.append((high << 4) | low)

        # Last digit and sign
        if len(digits) % 2 == 0:
            last_digit = int(digits[-1])
            result.append((last_digit << 4) | sign)
        else:
            # Odd number of digits, just add sign nibble
            result[-1] = (result[-1] & 0xF0) | sign

        return bytes(result)

    def decode_zoned_decimal(
        self,
        data: bytes,
        scale: int = 0,
    ) -> Decimal:
        """
        Decode zoned decimal (DISPLAY) to Decimal.

        Zoned decimal format:
        - Each digit occupies one byte
        - Zone nibble (high) is usually 0xF
        - Last byte zone contains sign (C=positive, D=negative)

        Args:
            data: Zoned decimal bytes
            scale: Number of decimal places

        Returns:
            Python Decimal value
        """
        if not data:
            return Decimal(0)

        # Extract digits and sign
        digits = []
        sign = 1

        for i, byte in enumerate(data):
            zone = byte >> 4
            digit = byte & 0x0F
            digits.append(str(digit))

            # Check sign in last byte
            if i == len(data) - 1:
                if zone == 0x0D:
                    sign = -1

        # Build number string
        num_str = "".join(digits)

        # Insert decimal point
        if scale > 0:
            if len(num_str) <= scale:
                num_str = "0" * (scale - len(num_str) + 1) + num_str
            num_str = num_str[:-scale] + "." + num_str[-scale:]

        result = Decimal(num_str)
        return result * sign

    def decode_binary(
        self,
        data: bytes,
        signed: bool = True,
    ) -> int:
        """
        Decode binary integer (COMP/COMP-4/COMP-5).

        Args:
            data: Binary bytes (big-endian)
            signed: Whether to interpret as signed

        Returns:
            Python integer
        """
        if not data:
            return 0

        return int.from_bytes(data, byteorder="big", signed=signed)


def convert_ebcdic_column(df, column_name: str, codec: str = "cp037"):
    """
    Convert an EBCDIC column in a DataFrame to UTF-8.

    This function is designed for use with PySpark DataFrames.

    Args:
        df: PySpark DataFrame
        column_name: Name of the column to convert
        codec: EBCDIC codec to use

    Returns:
        DataFrame with converted column
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    converter = EBCDICConverter(codec)

    @udf(StringType())
    def ebcdic_to_utf8(data):
        if data is None:
            return None
        if isinstance(data, bytes):
            return converter.decode(data)
        return str(data)

    return df.withColumn(column_name, ebcdic_to_utf8(df[column_name]))
