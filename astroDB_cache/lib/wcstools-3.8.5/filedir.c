/* File filedir.c
 * June 30, 2010
 * By Jessica Mink, Harvard-Smithsonian Center for Astrophysics
 * Send bug reports to jmink@cfa.harvard.edu

   Copyright (C) 2010
   Smithsonian Astrophysical Observatory, Cambridge, MA USA

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

static int verbose = 0;         /* verbose/debugging flag */
static int replace = 0;         /* character replacement flag */
static char c1, c2;
static void usage();

int
main (ac, av)
int ac;
char **av;
{
    char *fn;
    char *str;
    char *ext;
    int i, lroot;

    /* crack arguments */
    for (av++; --ac > 0 && *(str = *av) == '-'; av++) {
        char c;
        while ((c = *++str))
        switch (c) {

        case 'v':       /* more verbosity */
            verbose++;
            break;

        default:
            usage();
            break;
        }
    }

    /* There are ac remaining file names starting at av[0] */
    if (ac == 0)
        usage ();

    while (ac-- > 0) {
	fn = *av++;
	if (verbose)
    	    printf ("%s -> ", fn);
	ext = strrchr (fn, '/');
	if (ext != NULL) {
	    *ext = 0;
	    printf ("%s\n", fn);
	    }
	else
	    printf ("./\n");
	}

    return (0);
}

static void
usage ()
{
    fprintf (stderr,"FILEDIR: Return directory part of file pathname\n");
    fprintf(stderr,"Usage:  filedir file1 file2 file3 ...\n");
    exit (1);
}
/* Jun 30 2000	New program
 */
