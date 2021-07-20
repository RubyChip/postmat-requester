import requests
import sys
from typing import Union
from csv import DictWriter




class PostmatHandler:
    uri = ""
    token = ""

    def __init__(self):
        self._params: dict = {'page': 1}
        self._all_postmats: dict = {}

    def fetch_all_postmats(self) -> None:
        while True:
            response = requests.get(self.uri, headers={'Authorization': f'Token {self.token}'}, params=self._params)
            if response.status_code != 200:
                self._params['page'] = 0
                break
            result: list = response.json()['results']
            for postmat in result:
                fias_id: Union[str, dict]  = postmat.get('location', {}).get('address_struct', {}).get('fias_id', {})
                if not fias_id:
                    continue
                if fias_id not in self._all_postmats:
                    self._all_postmats.update({fias_id: []})
                self._all_postmats[fias_id].append(postmat['id'])
            self._params['page'] += 1

    def filter_postmats(self, limit: int) -> None:
        filtered_postmats = filter(lambda item: len(item[1]) < limit, self._all_postmats.items())
        self._all_postmats = {}
        for postmat in filtered_postmats:
            self._all_postmats.update({postmat[0]: postmat[1]})

    def fetch_postmats(self, limit: int = 0) -> dict:
        self.fetch_all_postmats()
        if limit:
            self.filter_postmats(limit)
        return self._all_postmats

    def output_csv(self, arr: dict) -> None:
        writer = DictWriter(sys.stdout, fieldnames=['id', 'fias_id'])
        writer.writeheader()
        for fias_id in arr:
            for id in arr[fias_id]:
                writer.writerow({'id': id, 'fias_id': fias_id})


if __name__ == '__main__':
    postmat = PostmatHandler()
    result = postmat.fetch_postmats(limit=10)
    postmat.output_csv(result)
