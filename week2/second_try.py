from prefect.filesystems import GitHub

gh = GitHub(repository="https://github.com/ruizcrp/dzc_homework.git")
gh.save("second")