import os
# tab-completion in command line

# Mac OS readline support requires serious hacking, cribbed from
# http://stackoverflow.com/questions/7116038/python-tab-completion-mac-osx-10-7-lion
import readline
import rlcompleter
if not readline.__doc__: # Windows
    COMPLETE_ABS=False
elif readline.__doc__ and 'libedit' in readline.__doc__: # MacOS
    readline.parse_and_bind("bind ^I rl_complete")
    COMPLETE_ABS=True
else: # *nix
    readline.parse_and_bind("tab: complete")
    COMPLETE_ABS=False

# lifting path completion from
# https://stackoverflow.com/questions/16826172/filename-tab-completion-in-cmd-cmd-of-python
def complete_path(text, line):
    arg = line.split()[1:]
    if not arg:
        completions = os.listdir('.'+os.sep)
    else:
        dir, part, base = arg[-1].rpartition(os.sep)
        if part == '':
            dir = '.'+os.sep
        elif dir == '':
            dir = os.sep          
        completions = []
        for f in os.listdir(dir):
            if f.startswith(base):
                cpath = os.path.join(dir,f)
                if COMPLETE_ABS:
                    addpath = cpath
                else:
                    addpath = f
                if os.path.isfile(cpath):
                    completions.append(addpath)
                else:
                    completions.append(addpath+os.sep)
    return completions
