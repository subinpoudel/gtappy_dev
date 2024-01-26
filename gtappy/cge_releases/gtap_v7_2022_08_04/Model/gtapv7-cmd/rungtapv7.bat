
::======================================================================
:: Step 1: Define aggregation name
::======================================================================
set agg=%1

::======================================================================
:: Step 2: Define cmf files
::======================================================================
set sim01=agpr20
set sim02=agpr20
set sim03=agpr20

::======================================================================
:: Step 3: Define Directories
::======================================================================
set MODd=..\mod
set SOLd=..\out
set CMFd=.\cmf
set DATd=..\data\%agg%

::======================================================================
:: Step 4: Run sims 
::======================================================================
cd %CMFd%

%MODd%\gtapv7 -cmf %sim01%.cmf -p1=%DATd% -p2=%SOLd%
     if errorlevel 1 goto error

goto skip
%MODd%\gtapv7 -cmf %sim02%.cmf -p1=%DATd% -p2=%SOLd%
     if errorlevel 1 goto error

%MODd%\gtapv7 -cmf %sim03%.cmf -p1=%DATd% -p2=%SOLd%
     if errorlevel 1 goto error

:skip

::====
:: END
::====

