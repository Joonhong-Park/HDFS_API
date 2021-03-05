package hdfs.jh.io;

import com.sun.jersey.core.header.ParameterizedHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import sun.nio.ch.IOUtil;

import javax.imageio.IIOException;
import java.beans.Transient;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;


public class hdfs_test {
    /** Configuration setting & get FileSystem
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static FileSystem getFileSystem(String nn) throws IOException, InterruptedException {

        /**
         * 기본적인 사용방법
         */
//        Configuration conf = new Configuration(false);
//        String uri = "hdfs://hdm1.cdp.jh.io";
//        FileSystem fs = FileSystem.get(URI.create(uri), conf, "impala");
//
//        return fs;

        /**
         * hdfs ha 적용시 사용방법
         */

//        Configuration conf = new Configuration();
//        String nameservices = "nn";
//        String[] namenodesAddr = {"hdm1.cdp.jh.io:8020", "hdm2.cdp.jh.io:8020"};
//        String[] namenodes = {"nn1", "nn2"};
//        conf.set("fs.defaultFS", "hdfs://" + nameservices);
//        conf.set("fs.default.name", conf.get("fs.defaultFS"));
//        conf.set("dfs.nameservices", nameservices);
//        conf.set("dfs.ha.namenodes." + nameservices, namenodes[0]+","+namenodes[1]);
//        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0]);
//        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1]);
//        conf.set("dfs.client.failover.proxy.provider." + nameservices,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//        FileSystem fs = FileSystem.get(conf);

//        FileSystem fs = FileSystem.get(conf);
//
//        return fs;

        /**
         * core-site.xml 작성 후 사용방법
         */
        // core-site.xml을 읽어와 객체 생성
        Configuration conf = new Configuration();
        // core-site.xml 의 "fs.defaultFS" 값을 연결하고자하는 네임노드값으로 변경
        conf.set("fs.defaultFS", "hdfs://" + nn);
        FileSystem fs = FileSystem.get(conf);

        return fs;

        /**
         * hdfs-site.xml 과 같이 다른 파일명으로 작성한 경우
         * 직접 Configuration 객체에 해당 파일 추가
         *
         */
//      Configuration conf = new Configuration();
//      conf.addResource("core-site.xml");
//      conf.set("fs.defaultFS", "hdfs://" + nn);
//      FileSystem fs = FileSystem.get(conf);
//      return fs;
    }



    /**
     * 해당 경로 디렉토리 생성
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param path 생성할 디렉토리 경로
     * @throws IOException
     */
    public static void makedir(FileSystem fs, String path) throws IOException{
        fs.mkdirs(new Path(path));
    }

    /**
     * 해당 경로 파일 목록 확인
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param input_path 확인할 디렉토리 경로
     * @throws IOException
     */
    public static void ls(FileSystem fs, String input_path) throws IOException{

        Path path = new Path(input_path);
        FileStatus[] files = fs.listStatus(path);
        for(FileStatus file : files){
            System.out.print(file.getPermission()+" ");
            // 심볼릭 링크 수
            System.out.print("-"+file.getOwner()+" ");
            System.out.print(file.getGroup()+" ");
            System.out.print(file.getBlockSize()+" ");
            System.out.print(file.getModificationTime()+" ");
            System.out.println("/"+file.getPath().getName()+" ");
        }
    }

    /**
     * 패턴을 사용한 해당 경로 파일 목록 확인
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param path 패턴이 적용된 경로
     * @throws IOException
     */
    public static void ls_pattern(FileSystem fs, Path path) throws IOException{
        FileStatus[] fileStatus = fs.globStatus(path);
        for (FileStatus file : fileStatus){
            System.out.println(file);
        }
    }

    /**
     * 파일 복사
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param file_path 복사할 파일 경로
     * @param dir_path 붙여넣을 디렉토리 경로
     * @throws IOException
     */
    public static void put(FileSystem fs, String file_path, String dir_path) throws IOException{
        fs.copyFromLocalFile(new Path(file_path), new Path(dir_path));
    }

    /**
     *
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param file_path 복사할 파일 경로
     * @param dir_path 붙여넣을 로컬 디렉토리 경로
     * @throws IOException
     */
    public static void cp(FileSystem fs, String file_path, String dir_path) throws IOException{
        fs.copyToLocalFile(new Path(file_path), new Path(dir_path));
    }

    /**
     * hdfs 파일을 읽어 내용 출력
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param file_path 읽을 파일 경로
     * @throws IOException
     */
    public static void cat(FileSystem fs, String file_path) throws IOException{
        FSDataInputStream is = fs.open(new Path(file_path));
        IOUtils.copyBytes(is, System.out, 4096, false);
        IOUtils.closeStream(is);
    }

    /**
     * 파일 권한 수정
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param file_path 읽을 파일 경로
     * @param perm 변경할 permission
     * @throws IOException
     */
    public static void chmod(FileSystem fs, String file_path, String perm) throws IOException{
        fs.setPermission(new Path(file_path), new FsPermission(perm));
    }

    /**
     * 파일 소유자 변경
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param file_path 읽을 파일 경로
     * @param own 변경할 username
     * @param group 변경할 group username
     * @throws IOException
     */
    public static void chown(FileSystem fs, String file_path, String own, String group) throws IOException{
        fs.setOwner(new Path(file_path), own, group);
    }

    /**
     * 파일 디렉토리 삭제 ( -skipTrash 가 포함된 기능 )
     * @param fs getFileSystem으로 가져온 filesystem객체
     * @param file_path 삭제할 파일 경로
     * @param recuresive true : 경로 내 모든 파일 삭제 (디렉토리 삭제), false : 해당 파일만 삭제
     * @throws IOException
     */
    public static void rm(FileSystem fs, String file_path, boolean recuresive) throws IOException{
        fs.delete(new Path(file_path), recuresive);
    }


    public static void main(String[] args) throws Exception{
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("impala");
        ugi.doAs((PrivilegedAction<Integer>) () -> {
            try {
                FileSystem filesystem = getFileSystem("nn");

                ls(filesystem, "/");

//                ls_pattern(filesystem, new Path("/????"));
//                ls_pattern(filesystem, new Path("/h*"));
//
//                makedir(filesystem, "/user/impala/test2");
//
//                put(filesystem, "C:/Users/joonhong/Documents/ipaddr.txt", "/tmp/");
//
//                cp(filesystem, "/tmp/ipaddr.txt", "C:/");
//
//                cat(filesystem, "/tmp/ipaddr.txt");
//
//                chmod(filesystem, "/tmp/ipaddr.txt", "755");
//
//                chown(filesystem, "/tmp/ipaddr.txt", "hdfs", "supergroup");
//
//                rm(filesystem, "/tmp/ipaddr.txt", false);
//                rm(filesystem, "/tmp/test*", true);

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            return 0;
        });
    }

}
