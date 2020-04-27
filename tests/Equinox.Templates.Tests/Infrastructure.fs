// Ported from https://github.com/Particular/dotnetTemplates/blob/master/src/Tests - Thanks @SimonCropp!
[<AutoOpen>]
module Equinox.Templates.Tests.Infrastructure

open System
open System.Diagnostics
open System.IO
open System.Runtime.CompilerServices

module Process =

    let direct (evt : IEvent<_, _>) output = evt.AddHandler(DataReceivedEventHandler(fun _sender args -> output args.Data))

    let run fileName args =
        let out, err = System.Text.StringBuilder(), System.Text.StringBuilder()
        let psi =
            ProcessStartInfo
                (   fileName, args, CreateNoWindow=true, WindowStyle=ProcessWindowStyle.Hidden,
                    UseShellExecute=false, RedirectStandardError=true, RedirectStandardOutput=true)
        use p = new Process(StartInfo = psi, EnableRaisingEvents=true)
        direct p.OutputDataReceived (out.AppendLine >> ignore); direct p.ErrorDataReceived (err.AppendLine >> ignore)
        let _wasFresh = p.Start()
        p.BeginErrorReadLine(); p.BeginOutputReadLine()
        if not (p.WaitForExit 10_000) then
            failwithf "Running %s %s timed out" fileName args
        if p.ExitCode <> 0 then
            failwithf "Process <%s %s> Failed:\nstdout: <%O>\n stderr: <%O>" fileName args out err

module Dotnet =

    let run cmd args =
        Process.run "dotnet" (System.String.Join(" ", cmd :: args))
    let install packagePath = run "new" [sprintf "-i \"%s\"" packagePath]
    let uninstall packageName = run "new" [sprintf "-u \"%s\"" packageName]
    let instantiate targetDirectory templateName args = run "new" ([templateName; sprintf "--output \"%s\"" targetDirectory] @ args)
    let build target = run "build" target

type CodeFolder =

    static member OfMe([<CallerFilePath>]?callerFilePath) =
        Directory.GetParent(callerFilePath |> Option.defaultWith (fun () -> failwith "Need folder")).FullName

module Dir =

    let (++) s1 s2 = Path.Combine(s1, s2)

    let projectBaseDir = CodeFolder.OfMe() ++ "../.."
    let projectBinNuGetDir = projectBaseDir ++ "bin/nupkg"

    let scratchFolder = projectBaseDir ++ "scratch-area"
    let cleared subFolder =
        let res = Path.GetFullPath(scratchFolder ++ subFolder)
        if Directory.Exists res then
            Directory.Delete(res, true)
        res

type EquinoxTemplatesFixture() =

    let [<Literal>] PackageName = "Equinox.Templates"
    let packagePath = Directory.EnumerateFiles(Dir.projectBinNuGetDir, PackageName + ".*.nupkg") |> Seq.sort |> Seq.last
    do Dotnet.install packagePath

    member val PackagePath = packagePath

    interface IDisposable with
        member __.Dispose() =
            Dotnet.uninstall PackageName
