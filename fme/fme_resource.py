from snowflake_analytics.resources.api import API
import httpx

class FMEResource(API):
    host: str
    api_key: str

    api_host: str = '/fmeapiv4'
    @property
    def auth_header(self):
        return {
            'Authorization': f'fmetoken token={self.api_key}'
        }
    
    
    def request_offset_paginated(self, endpoint: str, params: dict | None = None):
        base_url = f'{self.host}{self.api_host}{endpoint}'

        params = {} if params is None else params
        limit = params.get('limit', 1000)
        offset = params.get('offset', 0)

        # testing/dev
        # to limit iterations. testing shows around 3k rows so this shouldn't hit
        current_depth = 0
        max_depth = 20
        
        with httpx.Client(headers=self.auth_header) as client:
            while True:
                current_depth += 1
                if current_depth > max_depth:
                    raise RecursionError('Depth exceeded')

                request_param = params | {'limit': limit, 'offset': offset}

                response = self.get_request(
                    client=client, url=base_url, params=request_param
                )

                yield response

                # alternative is to base it on the `item` returning nothing
                j = response.json()
                response_tc = int(j.get('totalCount', 0))
                response_o = int(j.get('offset', 0))

                offset += limit

                if response_o >= response_tc:
                    break

