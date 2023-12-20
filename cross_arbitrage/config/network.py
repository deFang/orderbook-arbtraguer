from typing import Optional

from pydantic import BaseModel, root_validator


class NetworkConfig(BaseModel):
    http_proxy: Optional[str] = None
    https_proxy: Optional[str] = None

    def proxies(self):
        if not (self.http_proxy or self.https_proxy):
            return None
        ret = {}
        if self.http_proxy:
            ret['http'] = self.http_proxy
        if self.https_proxy:
            ret['https'] = self.https_proxy
        return ret

    @root_validator
    def update_http_proxy(cls, values):
        http_proxy = values.get("http_proxy")
        https_proxy = values.get("https_proxy")
        if http_proxy == "":
            http_proxy = None
        if https_proxy == "":
            https_proxy = None
        return values
