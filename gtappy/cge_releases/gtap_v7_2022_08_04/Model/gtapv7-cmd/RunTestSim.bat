
::======================================================================
:: Step 1: Define aggregation name
::======================================================================
set agg=%1
:: set agg=10x10

::======================================================================
:: Step 2: Define cmf files
::======================================================================
set sim01=gtapv7

::======================================================================
:: Step 3: Define Directories
::======================================================================
set MODd=..\mod
set SOLd=..\out
set CMFd=.\cmf
set DATd=..\data\%agg%

::======================================================================
:: Step 4: Run numeraire shock
::======================================================================
cd %CMFd%

%MODd%\gtapv7 -cmf %sim01%.cmf -p1=%DATd% -p2=%SOLd%
     if errorlevel 1 goto error
cd..

::======================================================================
:: Step 5: Run post-sim summary (GTAPview) and Shocks generation file
::======================================================================
cd mod

:: --------------------------------
:: Run GTAPView to generate summary 
:: --------------------------------
 gtpvewv7 -cmf gtpvewv7.cmf -p1=%DATd% -p2=%SOLd%
     if errorlevel 1 goto error

:: -------------------------------------------------------------
:: Run Shocksv7 to generate tax/subsidy shocks to eliminate them
:: ------------------------------------------------------------- 
 shocksv7 -cmf shocksv7.cmf -p1=%DATd% -p2=%SOLd%
     if errorlevel 1 goto error

cd..

::====
:: END
::====


