@ECHO OFF

pushd %~dp0

REM Command file for Sphinx documentation

REM C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\Scripts
echo f | xcopy /f /y ..\README.md README.md
REM echo f | xcopy /f /y css/overrides.css 

if "%SPHINXBUILD%" == "" (
	set SPHINXBUILD=set SPHINXBUILD="\\arcserver1\GIS\_Resources\ESRI\Python\Staging\VirtualEnvs\py_36_x64\Scripts\sphinx-build.exe"
)
set SOURCEDIR=.
set BUILDDIR=_build

if "%1" == "" goto help

%SPHINXBUILD% >NUL 2>NUL
if errorlevel 9009 (
	echo.
	echo.The 'sphinx-build' command was not found. Make sure you have Sphinx
	echo.installed, then set the SPHINXBUILD environment variable to point
	echo.to the full path of the 'sphinx-build' executable. Alternatively you
	echo.may add the Sphinx directory to PATH.
	echo.
	echo.If you don't have Sphinx installed, grab it from
	echo.http://sphinx-doc.org/
	exit /b 1
)

%SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
goto end

:help
%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%

:end
popd

set /p DUMMY=Hit ENTER to continue...