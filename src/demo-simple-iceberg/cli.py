"""
CLI for nviz
"""

import fire

from demo-simple-iceberg.main import show_message


class demo-simple-icebergCLI:
    def show_message(
        self,
        message: str = "Hello, world!",
    ) -> str:
        """
        CLI interface for show_message.

        Args:
            message (str):
                The message to print.
                Defaults to 'Hello, world!'.

        Returns:
            pd.DataFrame:
                A DataFrame containing the message.
        """

        # prints the message to screen
        print(show_message(message=message))


def trigger() -> None:
    """
    Trigger the CLI to run.
    """
    fire.Fire(demo-simple-icebergCLI)
