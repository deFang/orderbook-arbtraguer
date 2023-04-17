TERM_COLORS = {
    "black": "\033[30m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "magenta": "\033[35m",
    "cyan": "\033[36m",
    "white": "\033[37m",
    "underline": "\033[4m",
    "reset": "\033[0m",
}


def color(color, text):
    if TERM_COLORS.get(color):
        return f"{TERM_COLORS[color]}{text}{TERM_COLORS['white']}"

    # user default color
    return text
