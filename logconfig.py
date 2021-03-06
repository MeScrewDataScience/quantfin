def logging_config():
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(name)s - %(levelname)s - %(message)s"
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console_handler": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "level": "INFO"
            },
            "file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "detailed",
                "level": "INFO",
                "filename": "datafeed.log",
                "maxBytes": 10485760,
                "backupCount": 20,
                "encoding": "utf8"
            }
        },
        "loggers": {
            "": {
                "level": "DEBUG",
                "handlers": ["console_handler", "file_handler"],
                "filemode": "a"
            }
        }
    }

    return config