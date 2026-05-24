class Style:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"

def color(text: str, code: str) -> str:
    return f"{code}{text}{Style.RESET}"

class Cmd:
    def print_pass(self, text: str):
        print(color(text, Style.GREEN))

    def print_fail(self, text: str):
        print(color(text, Style.RED))