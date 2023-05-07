import yaml
import redis


def parse_yaml():
    with open("config/settings.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class DictAsMember(dict):
    """
    Converts yml to attribute for cleaner access
    """

    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsMember(value)
        return value


conf = DictAsMember(parse_yaml())
r = redis.Redis(host=conf.redis.host, port=conf.redis.port, decode_responses=True)
