# src/utils/singleton.py

from typing import Type, Dict


class SingletonMeta(type):
    """
    Metaclase que define el comportamiento Singleton. Al usar esta metaclase,
    cualquier clase que la herede solo tendrá una única instancia.
    """
    _instances: Dict[Type, object] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            # Si la clase no tiene una instancia, la crea y la almacena
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
