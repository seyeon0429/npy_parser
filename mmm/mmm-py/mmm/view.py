import numpy as np

class NasdaqMessageView:
    def __init__(self, array: np.ndarray) -> None:
        self._array = array

    @property
    def type(self):
        self._array[0]

    @property
    def time(self):
        self._array[1]


class ExecutionView(NasdaqMessageView):
    def __init__(self, array: np.ndarray) -> None:
        super().__init__(array)

    @property
    def reference(self):
        self._array[2]

    @property
    def executed_shares(self):
        self._array[3]

    @property
    def price(self):
        self._array[4]

    @property
    def side(self):
        self._array[5]

    @property
    def original_shares(self):
        self._array[6]

    @property
    def next_index(self):
        self._array[8]


def view(array: np.ndarray) -> NasdaqMessageView:
    if array[0] == 4:
        return ExecutionView(array)
    else:
        NotImplementedError()
