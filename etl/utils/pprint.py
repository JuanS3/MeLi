"""
Here you can find a pretty print just for trace events in console.
"""

class _ColorBase:
    """
    This class is used as a base class for the color classes.

    It contains the methods for starting and ending the color formatting.
    """
    RESET_FORMAT_COLOR: int = 0
    START_FORMAT_COLOR: str = '\033[{}m'
    END_FORMAT_COLOR: str   = START_FORMAT_COLOR.format(RESET_FORMAT_COLOR)

    def start(self, color):
        return self.START_FORMAT_COLOR.format(color)

    def end(self):
        return self.END_FORMAT_COLOR

    def reset(self):
        return self.END_FORMAT_COLOR


class ColorTextCode(_ColorBase):
    """
    This class is used to store the color codes for the text.
    """
    BLACK_CODE: int   = 30
    RED_CODE: int     = 31
    GREEN_CODE: int   = 32
    YELLOW_CODE: int  = 33
    BLUE_CODE: int    = 34
    MAGENTA_CODE: int = 35
    CYAN_CODE: int    = 36
    WHITE_CODE: int   = 37


class ColorText(ColorTextCode):
    """
    This class is used to color the text.

    Example usage:
        >>> from consoleverse.term.colors import ColorText
        >>> text = ColorText()
        >>> print(text['RED'] + 'Hello ConsoleVerse!' + text.reset())

    The above code will print 'Hello ConsoleVerse!' in red.

    The following are the available text colors:
        - BLACK
        - RED
        - GREEN
        - YELLOW
        - BLUE
        - MAGENTA
        - CYAN
        - WHITE
    """
    BLACK   = 'BLACK'
    RED     = 'RED'
    GREEN   = 'GREEN'
    YELLOW  = 'YELLOW'
    BLUE    = 'BLUE'
    MAGENTA = 'MAGENTA'
    CYAN    = 'CYAN'
    WHITE   = 'WHITE'

    def __init__(self):
        self.COLORS = {
            self.BLACK   : self.start(self.BLACK_CODE),
            self.RED     : self.start(self.RED_CODE),
            self.GREEN   : self.start(self.GREEN_CODE),
            self.YELLOW  : self.start(self.YELLOW_CODE),
            self.BLUE    : self.start(self.BLUE_CODE),
            self.MAGENTA : self.start(self.MAGENTA_CODE),
            self.CYAN    : self.start(self.CYAN_CODE),
            self.WHITE   : self.start(self.WHITE_CODE),
        }

        self.COLORS_LIST = list(self.COLORS.keys())

    def __getitem__(self, color):
        return self.COLORS[color.upper()]

    def __contains__(self, color):
        if color:
            return color.upper() in self.COLORS

    def __str__(self):
        return str(self.COLORS_LIST)


class StyleTextCode(_ColorBase):
    """
    This class is used to store the style codes for the text.
    """
    BOLD_CODE: int      = 1
    DIM_CODE: int       = 2
    UNDERLINE_CODE: int = 4
    BLINK_CODE: int     = 5
    REVERSE_CODE: int   = 7
    HIDDEN_CODE: int    = 8


class StyleText(StyleTextCode):
    """
    This class contains the styles for the text.

    Example usage:
        >>> from consoleverse.term.colors import StyleText
        >>> style = StyleText()
        >>> print(style['BOLD'] + 'Hello ConsoleVerse' + style.end())

    The above code will print 'Hello ConsoleVerse' in bold.

    The styles are:
        - BOLD
        - UNDERLINE
        - BLINK
        - REVERSE
        - HIDDEN
    """
    BOLD      = 'BOLD'
    DIM       = 'DIM'
    UNDERLINE = 'UNDERLINE'
    BLINK     = 'BLINK'
    REVERSE   = 'REVERSE'
    HIDDEN    = 'HIDDEN'

    def __init__(self):
        self.STYLES = {
            self.BOLD      : self.start(self.BOLD_CODE),
            self.DIM       : self.start(self.DIM_CODE),
            self.UNDERLINE : self.start(self.UNDERLINE_CODE),
            self.BLINK     : self.start(self.BLINK_CODE),
            self.REVERSE   : self.start(self.REVERSE_CODE),
            self.HIDDEN    : self.start(self.HIDDEN_CODE),
        }

        self.STYLES_LIST = list(self.STYLES.keys())

    def __getitem__(self, style):
        return self.STYLES[style.upper()]

    def __contains__(self, style):
        return style.upper() in self.STYLES

    def __str__(self):
        return str(self.STYLES_LIST)


def title(msg: str):
    size: int = len(msg)
    line: str = '⎯' * size
    style = StyleText()
    print(style['bold'] + msg)
    print(line)


def info(msg: str):
    color = ColorText()
    symbol: str = color['BLUE'] + '➜' + color.reset()
    print(f' {symbol}  {msg}')


def warning(msg: str):
    color = ColorText()
    symbol: str = color['YELLOW'] + '⚠' + color.reset()
    print(f' {symbol} {msg}')


def error(msg: str):
    color = ColorText()
    symbol: str = color['RED'] + '✘' + color.reset()
    print(f' {symbol} {msg}')


def success(msg: str):
    color = ColorText()
    symbol: str = color['GREEN'] + '✔' + color.reset()
    print(f' {symbol} {msg}')


def debug(msg: str):
    color = ColorText()
    symbol: str = color['CYAN'] + '➤' + color.reset()
    print(f' {symbol} {msg}')


if __name__ == '__main__':
    color = ColorText()
    style = StyleText()

    msg: str = f'Hello {color["yellow"]}Me{color["BLUE"]}Li{style.end()}'
    title(msg)

