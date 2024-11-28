class CustomFunctionError(Exception):
    """
    Excepción personalizada para capturar errores con códigos específicos.
    """

    def __init__(self, code, error_details, is_technical_error=False):
        self.code = code
        self.error_details = error_details
        self.is_technical_error = is_technical_error
        super().__init__(f"[Error Code {code}]: {error_details}")
