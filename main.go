package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Mysql  MysqlConfig
	Local  LocalConfig
	Remote RemoteConfig
	File   FileConfig
}

type MysqlConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	Name     string
}
type FileConfig struct {
	Path string
}
type LocalConfig struct {
	Dir string
}
type RemoteConfig struct {
	Host       string
	User       string
	Password   string
	PrivateKey string
	Dir        string
}

func main() {
	log.Println("Start the backup program")
	run()
}

func init() {
	log.SetPrefix("[DBK]")

}

func run() {
	config := getConfig()
	timestamp := getCurrentTimestamp()
	localDir := path.Join(config.Local.Dir, timestamp)
	createPath(localDir)
	err := backupMysql(&config.Mysql, localDir)
	if err != nil {
		log.Fatalln(err)
	}
	err = backupFile(&config.File, localDir)
	if err != nil {
		log.Fatalln(err)
	}
	tarFile := path.Join(config.Local.Dir, timestamp+".tar.gz")
	CompressingDir(tarFile, localDir)
	if strings.TrimSpace(config.Remote.PrivateKey) != "" {
		//remoteDir := path.Join(config.Remote.Dir)
		err = remoteSyncForRSA(&config.Remote, tarFile)
		if err != nil {
			log.Fatalln(err)
		}
		removeAllContents(config.Local.Dir)
	} else if strings.TrimSpace(config.Remote.User) != "" {
		err = remoteSyncForUser(&config.Remote, tarFile)
		if err != nil {
			log.Fatalln(err)
		}
		removeAllContents(config.Local.Dir)
	}

}

func getConfig() *Config {
	exePath, err := os.Executable()
	if err != nil {
		panic(fmt.Errorf("failed acquisition run path: %w", err))
	}
	dirPath, err := filepath.EvalSymlinks(filepath.Dir(exePath))
	if err != nil {
		panic(fmt.Errorf("Failed fetch path	: %w", err))
	}
	configPath := path.Join(dirPath, "config.yaml")
	dataBytes, err := os.ReadFile(configPath)
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	config := Config{}
	err = yaml.Unmarshal(dataBytes, &config)
	if err != nil {
		panic(fmt.Errorf("unable to decode into struct, %v", err))
	}

	//fmt.Printf("map → %+v", config)
	return &config
}

func backupMysql(mysqlConfig *MysqlConfig, backupDir string) error {
	fileName := mysqlConfig.Name + ".sql"
	outputPath := path.Join(backupDir, fileName)

	argsCmd := []string{
		"-h" + mysqlConfig.Host,
		"-P" + mysqlConfig.Port,
		"-u" + mysqlConfig.User,
		"-p" + mysqlConfig.Password,
		mysqlConfig.Name,
	}
	log.Printf("Backing up db %s to %s\n", mysqlConfig.Name, outputPath)
	cmd := exec.Command("mysqldump", argsCmd...)
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("Failed to create the output file: %v", err)
	}
	defer file.Close()

	cmd.Stdout = file

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("mysqldump Command execution failed: %v", err)
	}
	log.Printf("Backup completed successfully:%s\n", outputPath)
	return nil
}
func backupFile(fileConfig *FileConfig, backupDir string) error {
	// Check if the directory exists
	if _, err := os.Stat(fileConfig.Path); os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", fileConfig.Path)
	}

	// Generate a timestamp to include in the backup file name
	cmd := exec.Command("rsync", "-a", fileConfig.Path, backupDir)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to create backup: %v", err)
	}

	log.Printf("Backup completed successfully: \n")
	return nil

}

func CompressingDir(tarFilePath string, compressPath string) error {

	log.Printf("Backing up directory %s to %s\n", compressPath, tarFilePath)

	// Run the tar command to create a compressed archive of the directory
	cmd := exec.Command("tar", "-czf", tarFilePath, "-C", compressPath, ".")
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Failed compression directory: %v", err)
	}
	err = os.Chmod(tarFilePath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Failure to raise rights: %v", err)
	}
	log.Printf("Compressed directory successfully: %s\n", tarFilePath)
	return nil

}

func remoteSyncForUser(remoteConfig *RemoteConfig, backupPath string) error {
	// Ensure that at least one backup file is specified

	// Build the rsync command to sync files to the remote host
	rsyncArgs := []string{
		"-p",
		remoteConfig.Password,
		"rsync",
		"-av",
	}

	// Add each backup file to the rsync command
	rsyncArgs = append(rsyncArgs, backupPath)
	// Destination in the format user@host:dir
	destination := fmt.Sprintf("%s@%s:%s", remoteConfig.User, remoteConfig.Host, remoteConfig.Dir)
	rsyncArgs = append(rsyncArgs, destination)

	log.Printf("Syncing files to remote host using rsync: %s\n", strings.Join(rsyncArgs, " "))

	// Run the rsync command
	cmd := exec.Command("sshpass", rsyncArgs...)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to sync files to remote host: %v", err)
	}

	log.Println("Sync completed successfully")
	return nil
}
func remoteSyncForRSA(remoteConfig *RemoteConfig, backupPath string) error {
	// Ensure that at least one backup file is specified

	// Build the rsync command to sync files to the remote host
	rsyncArgs := []string{
		"-avz",
		"-e",
		"ssh -i " + remoteConfig.PrivateKey, // Replace /path/to/private/key with the actual path to the private key
	}

	// Add each backup file to the rsync command
	rsyncArgs = append(rsyncArgs, backupPath)
	// Destination in the format user@host:dir
	destination := fmt.Sprintf("%s@%s:%s", remoteConfig.User, remoteConfig.Host, remoteConfig.Dir)
	rsyncArgs = append(rsyncArgs, destination)

	log.Printf("Syncing files to remote host using rsync: %s\n", strings.Join(rsyncArgs, " "))

	// Run the rsync command
	cmd := exec.Command("rsync", rsyncArgs...)
	err := cmd.Run()
	// output, err := cmd.CombinedOutput()
	// if err != nil {
	// 	return fmt.Errorf("failed to sync files to remote host: %v, output: %s", err, output)
	// }
	if err != nil {
		return fmt.Errorf("failed to sync files to remote host: %v", err)
	}

	log.Println("Sync completed successfully")
	return nil
}

func removeAllContents(dirPath string) error {
	// 获取目录下的所有文件和子文件夹
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	// 删除文件和子文件夹
	for _, entry := range entries {
		entryPath := filepath.Join(dirPath, entry.Name())

		// 如果是目录，递归删除
		if entry.IsDir() {
			err := removeAllContents(entryPath)
			if err != nil {
				return err
			}

			// 删除空目录
			err = os.Remove(entryPath)
			if err != nil {
				return err
			}
		} else {
			// 如果是文件，直接删除
			err := os.Remove(entryPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getCurrentTimestamp() string {
	return time.Now().Format("20060102_150405")
}

func createPath(path string) {

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Fatalf("error:%s\n", err)
		}
		log.Printf("Create folder %s\n", path)
	}
}
