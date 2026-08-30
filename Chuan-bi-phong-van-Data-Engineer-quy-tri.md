## **Quy trình phỏng vấn sơ bộ ở Grab** **Vòng đầu tiên là bài test chín mươi phút HackerRank.**

 Thì bài test này nó bao gồm ba câu giải thuật và một câu SQL. Chỉ những bạn nào hoàn thành trên tám mươi phần trăm mới được gọi vào vòng thứ hai.

Vòng thứ hai nó là whiteboard coding. Thì ở vòng này nó sẽ chia ra làm hai phần nhỏ. Phần một vẫn là algorithms. Các bạn sẽ sử dụng mã giả để giải các bài toán algorithms trên bảng. Thì giám khảo sẽ yêu cầu các bạn tối ưu hóa câu trả lời của mình. Ví dụ như độ phức tạp complexity thay vì là O(n) thì các bạn phải đưa ra được O(n log n) chẳng hạn. 

Ở phần thứ hai sẽ là về system design. Thì họ sẽ đưa ra một số bài toán kinh điển, ví dụ như là thiết kế thang máy, thiết kế máy bán hàng tự động hoặc thiết kế ứng dụng Uber. 

Thì tổng thời gian cho cái vòng thứ hai này nó sẽ rơi vào đâu đó khoảng hai tiếng đồng hồ. Thật ra hai vòng này nó khá cơ bản mà hầu như bạn nào cũng nên chuẩn bị vì nó áp dụng cho mọi vị trí engineer.

 Ví dụ như back-end, software engineer hay thậm chí cả là machine learning engineer nữa. Thì sau khi mà bạn qua được vòng thứ hai thì các bạn sẽ gặp data engineer. 

Thông thường mình là người phỏng vấn ở vòng thứ ba và cũng chính là vòng data engineer. 

Thì cuộc phỏng vấn này của mình nó sẽ giống như một buổi thảo luận để hai bên có thể hiểu lẫn nhau. 

Ở vòng này, bản thân mình rất thích hỏi về kinh nghiệm làm việc của ứng viên.   
Mình sẽ yêu cầu các bạn kể sơ qua một số projects hoặc công việc nào đó mà bạn tâm đắc nhất.   
Ví dụ như nếu mà bạn nào đó mới ra trường thì có thể sẽ là một project nào đó liên quan với data engineer. 

Hoặc là bạn nào đã có kinh nghiệm thì đa số là công việc hiện tại bạn đó đang làm.   
Trong quá trình mà thảo luận cái đó thì mình sẽ đưa ra một số câu hỏi liên quan tới cái projects hoặc công việc đó để mình hiểu rõ hơn về những kỹ năng của ứng viên cũng như xem thử là thực sự bạn đó có làm công việc hay không. 

Cần phải tránh lỗi : “””Tại vì thật ra mình gặp rất là nhiều bạn, họ chỉ tham gia vào một project mà có bốn hoặc năm thành viên và chỉ làm một phần nhỏ nhưng mà lúc mà đi phỏng vấn thì chém như đúng rồi. “”” 

Sau khi hiểu rõ rồi mình mới chuyển sang kiến thức chuyên môn. Hầu như sẽ không ai hỏi về tool mà thay vào đó sẽ hỏi về kiến thức cơ bản, kể cả mình cũng vậy. Thì ví dụ như mình sẽ hỏi về Map-Reduce, đại loại như: 

mình sẽ yêu cầu ứng viên code một đoạn nhỏ trên giấy, trên máy tính hay là trên bảng trắng về Map-Reduce. 

Bài toán có thể là: Đọc số lượng chữ xuất hiện trong một câu. Đại loại như vậy và ứng viên phải viết ngay lập tức và tại chỗ cho mình. 

Câu hỏi 1: \*\*\* Một câu khác mà mình khá thích và mình hay đề cập ở các video trước đó là về tối ưu hóa Spark job. Ví dụ như mình có một cái Spark job, ờ... convert một column có định dạng là year, month, day sang ba column khác nhau ngày, tháng, năm. Và cái Spark job này nó sẽ xử lý trên một bộ data nặng năm mươi terabytes. Thì mình sẽ yêu cầu ứng viên làm cách nào để tối ưu hóa cho mình cái Spark job này để nó chạy với thời gian ít nhất. \*\*\*

Câu hỏi 2: \*\*\* Một câu hỏi nữa là mình sẽ cho ứng viên một cái schema và toàn bộ thông tin của schema đó. Sau đó mình sẽ yêu cầu ứng viên thiết kế cho mình một cái data mart và hoặc là data warehouse dựa theo requirement là A, B, C, D nào đó. \*\*\*

Thì mục đích của cái câu hỏi này là mình muốn xem thử xem một bạn apply vào vị trí data engineer thì bạn đó có biết về data warehouse hay không và bạn đó sẽ xây dựng data warehouse như thế nào. 

Cuối cùng thì mình sẽ trao đổi với ứng viên về phần architect design. Thì thường mình sẽ đưa ra một vấn đề và yêu cầu bạn đó thiết kế một cái data pipeline. Trong quá trình này, mình và ứng viên sẽ thảo luận về requirements, những lựa chọn của bạn đó, tại sao bạn đó lại chọn công cụ A mà không chọn công cụ B. Mục đích chính á là mình muốn biết được là bạn đó quen thuộc với bao nhiêu công cụ và bạn đó ứng dụng những cái công cụ đó vào giải quyết vấn đề như thế nào. Thì nó sẽ khá là thiết thực khi mà bạn đó join vào team và làm một công việc hằng ngày.

Đây là một quá trình mà hoàn chỉnh để phỏng vấn data engineer, nhưng mà tùy theo vị trí mà yêu cầu có thể khác nhau. Ví dụ như mình tuyển fresher thì mình sẽ chủ yếu xem thử về kiến thức nền tảng của bạn đó và khả năng học hỏi của bạn đó khi mà bạn đó vào team. Còn nếu mà mình tuyển junior hoặc mid-level thì sẽ yêu cầu bạn đó là có nhiều kinh nghiệm thực tế hơn là những kinh nghiệm lý thuyết, kiến thức nền tảng phải vững, thì cái yêu cầu nó sẽ cao hơn rất là nhiều. Bởi vậy cái kiến thức nền tảng nó rất là quan trọng khi mà bạn nào muốn đi phỏng vấn data engineer. Và khi mà đi phỏng vấn thì nên chuẩn bị cho mình một cái tâm lý rất là thoải mái và thư giãn để có thể là trả lời được tất cả các câu hỏi mà bên nhà tuyển dụng đặt ra.

## **Kỹ Sư Dữ Liệu \- [Data Engineer](https://www.topcv.vn/tim-viec-lam-data-engineer) \- Khối Dữ Liệu (2026TD450985) \- ngân hàng MB**

## **Mô tả công việc**

* Thực hiện công việc tích hợp, phát triển dữ liệu, hỗ trợ vận hành hệ thống công nghệ dữ liệu trong mảng chức năng được giao đảm bảo đúng quy trình, quy định của MB nhằm thực hiện thành công mục tiêu chung của đơn vị.  
* Tự phát triển hoặc phối hợp cùng các nhà cung cấp phát triển/triển khai các giải pháp Công nghệ dữ liệu theo các công việc được phân công.  
* Duy trì hoạt động của các sản phẩm/dịch vụ được phân công.  
* Nghiên cứu, tìm kiếm, tham mưu các giải pháp/sáng kiến Công nghệ dữ liệu và đề xuất nhà cung cấp đáp ứng yêu cầu của công việc.  
* Phát triển, kiểm thử (Unit test), nghiệm thu và hỗ trợ mức 2 (Level 2\) các sản phẩm và dịch vụ Công nghệ dữ liệu

## **Yêu cầu ứng viên**

* Tốt nghiệp Đại học các chuyên ngành Công nghệ thông tin, Điện tử-Viễn thông, Toán Tin, Khoa học máy tính, Khoa học dữ liệu,...  
* Ưu tiên ứng viên có một trong các chứng chỉ nghề quốc tế về lập trình phát triển dữ liệu về Oracle (Oracle Certified Associate (OCA), Oracle Certified Professional (OCP), Oracle Certified Master (OCM), Oracle Certified Expert (OCE) và Specialist cho Oracle Database 12c...), Netezza, BI Tableau, BI publisher, Cloud ( AWS Certified Solutions Architect, AWS Certified Developer – Associate, ...).  
* TOEIC 450 hoặc chứng chỉ tương đương.

## **CÔNG TY LOGISTICS \- TỔNG CÔNG TY BƯU ĐIỆN VIETNAM POST**

### Mô tả công việc

**Xây dựng và vận hành nền tảng dữ liệu trung tâm (Data Foundation)**

* Thiết kế và triển khai kiến trúc Data Lake trên Azure SQL / Microsoft Fabric để nhận dữ liệu vận hành từ nền tảng vận hành Logistics (VLP; hệ thống WMS, TMS, OMS, BMS, FMS, Finance, CRM).  
* Xây dựng Master Data Management (MDM-lite): danh mục chuẩn về khách hàng, SKU, địa điểm và nhà vận chuyển nhằm đảm bảo tính nhất quán dữ liệu trên toàn hệ thống.  
* Thiết lập quy trình giám sát và kiểm tra chất lượng dữ liệu, duy trì mức data quality \>95% – điều kiện tiên quyết trước khi triển khai AI và các dịch vụ phân tích.  
* Tài liệu hóa data schema, data dictionary, data lineage và SLA cho toàn bộ pipeline dữ liệu.  
  **Phát triển và duy trì pipeline tích hợp dữ liệu (Integration Bus)**  
* Là đầu mối kỹ thuật nội bộ trong quá trình kết nối API VLP: nghiên cứu API documentation từ Tổng công ty, phối hợp với đội ngũ thực hiện để đảm bảo tích hợp đúng chuẩn và đúng tiến độ.  
* Xây dựng và vận hành các workflow tự động (Azure Data Factory / Logic Apps) để đồng bộ dữ liệu từ hệ thống vận hành vào Data Lake theo lịch trình hoặc gần thời gian thực.  
* Phát triển và bảo trì ETL/ELT pipeline từ các nguồn dữ liệu nội bộ: hệ thống vận hành, CRM, eGlobal, ERP và các đối tác B2B.  
* Theo dõi, xử lý sự cố, tối ưu hiệu suất và đảm bảo tính ổn định của toàn bộ hệ thống pipeline dữ liệu.  
  **Hỗ trợ phân tích dữ liệu và chuẩn bị cho AI**  
* Chuẩn bị, làm sạch và cấu trúc dữ liệu phục vụ các dashboard Power BI của đội Analytics BI (B4); xây dựng semantic layer và data model phù hợp với từng trục cột kinh doanh (Logistics tổng quát, Sản xuất, TMĐT, Dược phẩm).  
* Phối hợp với đội vận hành để xây dựng dashboard dữ liệu để cấp quản lý theo dõi tiến trình xây dựng nền tảng.  
  **Phối hợp với nhà cung cấp và quản lý dự án dữ liệu**  
* Là đầu mối kỹ thuật nội bộ trong các buổi làm việc với nhà cung cấp giải pháp dữ liệu; kiểm tra và nghiệm thu sản phẩm bằng giao theo đúng tiêu chuẩn chất lượng và kiến trúc đã thống nhất.  
* Theo dõi tiến độ, lập báo cáo định kỳ về tình trạng xây dựng hạ tầng dữ liệu cho Trưởng nhóm và Ban Lãnh đạo.  
* Nghiên cứu và đề xuất công nghệ, công cụ dữ liệu mới phù hợp với lộ trình kỹ thuật và ngân sách của VNPL.  
  **Thực hiện các nhiệm vụ khác theo yêu cầu của cấp trên và Ban Lãnh đạo.**

### Yêu cầu ứng viên

* Học vấn: Tốt nghiệp Đại học trở lên ngành Khoa học Máy tính, Kỹ thuật Phần mềm, Hệ thống Thông tin, Toán – Tin học hoặc các ngành kỹ thuật có liên quan.  
* Kỹ năng cần thiết:  
* SQL thành thạo (Microsoft SQL Server, PostgreSQL): truy vấn phức tạp, tối ưu hóa query, xây dựng stored procedure và view.  
* Python cho xử lý dữ liệu và xây dựng pipeline (pandas, PySpark hoặc tương đương).  
* Kinh nghiệm với hệ sinh thái Microsoft Azure: Azure SQL, Azure Data Factory, Logic Apps, Microsoft Fabric; hoặc nền tảng cloud tương đương (AWS Glue/Redshift, GCP BigQuery).  
* Hiểu biết về Data Warehouse, Data Lake, ETL/ELT, Data Modeling (Star Schema, Snowflake Schema).  
* Kinh nghiệm tích hợp API REST/SOAP: đọc API documentation, test với Postman/Insomnia, xử lý lỗi và retry logic.  
* Kỹ năng tài liệu hóa kỹ thuật: viết data dictionary, data flow diagram, SLA documentation – cẩn thận và có hệ thống.  
* Tư duy hệ thống, chú trọng chất lượng dữ liệu, chủ động phát hiện và xử lý vấn đề trước khi leo thang.  
* Giao tiếp tốt với cả bên kỹ thuật (vendor, IT) và bên nghiệp vụ (Business, Operations); khả năng làm việc độc lập trong môi trường startup/build-phase.  
* Ưu tiên: Kinh nghiệm với Microsoft Fabric / Power BI semantic layer; hiểu biết về quy trình logistics hoặc chuỗi cung ứng.  
* Kinh nghiệm: Tối thiểu 2–4 năm kinh nghiệm ở vị trí Data Engineer, Data Analyst kỹ thuật hoặc ETL/Integration Developer; ưu tiên ứng viên có kinh nghiệm ngành Logistics, TMĐT hoặc Tài chính.  
* Ngoại ngữ: Tiếng Anh: Đọc hiểu tài liệu kỹ thuật bắt buộc; giao tiếp là lợi thế.

## [**Data Engineer**](https://www.topcv.vn/tim-viec-lam-data-engineer) **\- ID2608 \- ngân hàng VP Bank**

## **Tóm tắt: Yêu cầu: 3 năm kinh nghiệm chuyên mônĐại Học trở lên**

## **Chuyên môn: [Chuyên môn Data Engineer](https://www.topcv.vn/tim-viec-lam-data-engineer-cr257cb261cl285)[IT \- Phần mềm](https://www.topcv.vn/tim-viec-lam-moi-nhat?domain_knowledge=3)**

## **Địa điểm làm việc** *(đã được cập nhật theo Danh mục Hành chính mới \- thêm quận/huyện cũ tương ứng để dễ dàng tra cứu)*

## **\- Hà Nội: 89 Láng Hạ, Phường Đống Đa (quận Đống Đa cũ)**

## **Mô tả công việc**

\- Phân tích yêu cầu nghiệp vụ cho hệ thống dữ liệu và báo cáo (đối với kỹ sư dữ liệu toàn phần)

\- Phân tích thiết kế luồng dữ liệu cho từng CR các phân vùng dữ liệu/ báo cáo của DPC nhằm đáp ứng yêu cầu nghiệp vụ. Tham gia dựng môi trường, thực hiện lập trình phát triển, kiểm thử đơn lẻ (unit test)

\- Tham gia golive các yêu cầu thay đổi hệ thống

\- Tổ chức liên tục cải tiến tối ưu về mặt thiết kế, góp phần làm giảm thời gian xử lý dữ liệu cũng như tài nguyên sử dụng trên các hệ thống dữ liệu

\- Tham vấn cho các đơn vị nghiệp vụ và khối CNTT về giải pháp xử lý khắc phục khi xảy ra các lỗi phức tạp. Hỗ trợ phân tích xử lý lỗi vận hành hệ thống khi cần áp dụng kỹ thuật xử lý dữ liệu chuyên sâu.

\- Nâng cấp phần mềm, cài đặt triển khai các tầng ứng dụng. 

\- Với một số hệ thống ứng dụng có thể cần quản lý trực tiếp việc vận hành dữ liệu và báo cáo

## **Yêu cầu ứng viên**

\- Tốt nghiệp đại học hoặc cao hơn từ xếp loại Khá trở lên các chuyên ngành: Khoa học dữ liệu, Khoa học máy tính, Công nghệ thông tin, Toán tin ứng dụng, điện tử viễn thông hoặc tương đương.

\- Tối thiểu 3 năm trong lĩnh vực quản lý dữ liệu/báo cáo/phân tích/học máy

\- Truy vấn, tổ chức, phân tích dữ liệu

\- Sử dụng các công nghệ cập nhật để xử lý dữ liệu để phát triển và vận hành

\- Phân tích yêu cầu nghiệp vụ ngân hàng

\- Kiểm thử dữ liệu và phần mềm tài chính ngân hàng

\- Triển khai cùng và quản lý đối tác công nghệ thông tin

\- Cơ sở dữ liệu quan hệ RDBMS, NoSQL 

\- Thiết kế lập trình cho các hệ thống như hệ thống Data warehouse, Data lake, lakehouse, API, CI/CD. các dịch vụ xử lý dữ liệu và luồng dữ liệu dạng batch/stream, quản lý luồng vận  hành ETL, ELT: SSIS, Datastage, ODI, Kafka, Spark, Nifi, Airflow, DBT, Databrick, Redshift v.v...

\- Kiến thứ về xây dựng, tổ chức luồng dữ liệu (Batching, Streaming)

\- Kiến thức về lập trình, cấu trúc dữ liệu và giải thuật.

\- Có chứng chỉ chuyên môn trong lĩnh vực Data Engineering/ Software Development / Data Architect được cấp bởi các tổ chức lớn như Google, Cloudera, IBM, Dasca, Databricks... là một điểm cộng.

\- Có kinh nghiệm với ít nhất 1 trong các BI Tool là lợi thế

## **Quyền lợi được hưởng**

\- Thu nhập hấp dẫn, lương thưởng cạnh tranh theo năng lực  
\- Thưởng các Ngày lễ, Tết (theo chính sách ngân hàng từng thời kỳ)  
\- Được vay ưu đãi theo chính sách ngân hàng từng thời kỳ  
\- Chế độ ngày phép hấp dẫn theo cấp bậc công việc  
\- Bảo hiểm bắt buộc theo luật lao động \+ Bảo hiểm VPBank care cho CBNV tùy theo cấp bậc và thời gian công tác  
\- Được tham gia các khóa đào tạo tùy thuộc vào Khung đào tạo cho từng vị trí  
\- Thời gian làm việc: từ thứ 2 – thứ 6 & 2 sáng thứ 7/ tháng  
\- Môi trường làm việc năng động, thân thiện, có nhiều cơ hội học đào tạo, học hỏi và phát triển; được tham gia nhiều hoạt động văn hóa thú vị (cuộc thi về thể thao, tài năng, hoạt động teambuiding...)

## [**Data Engineer**](https://www.topcv.vn/tim-viec-lam-data-engineer) **\- TA192 Ngân hàng VP Bank**

## **Tóm tắt**

Yêu cầu:

3 năm kinh nghiệm chuyên mônĐại Học trở lên

Chuyên môn:

[Chuyên môn Data Engineer](https://www.topcv.vn/tim-viec-lam-data-engineer-cr257cb261cl285)[IT \- Phần cứng và máy tính](https://www.topcv.vn/tim-viec-lam-moi-nhat?domain_knowledge=2)[IT \- Phần mềm](https://www.topcv.vn/tim-viec-lam-moi-nhat?domain_knowledge=3)[Điện toán đám mây (Cloud)](https://www.topcv.vn/tim-viec-lam-moi-nhat?domain_knowledge=6)

## **Địa điểm làm việc** *(đã được cập nhật theo Danh mục Hành chính mới \- thêm quận/huyện cũ tương ứng để dễ dàng tra cứu)*

\- **Hà Nội:** Phường Đống Đa (quận Đống Đa cũ)

## **Mô tả công việc**

* To be able to develop/design for Data Lake, System/Data Integration, App engine solutions (containing complex big data computations), ensure the product delivered on time, within budget.  
* Work with Data Expert to align architecture and solution.  
* Develop, optimize, and maintain scalable ETL/ELT pipelines using AWS services such as Glue, Lambda, and Kinesis.  
* Build and manage data storage solutions leveraging AWS S3, Redshift, RDS, DynamoDB, and Athena.  
* Design data ingestion workflows from diverse data sources ensuring data quality and integrity.  
* To make sure all incidents which cannot be solved on previous lines of support are solved (Level 3 support)  
* Provide Data integration standards for new IT projects within Data domain.  
* Research and recommend future-proofed Data integration/processing technologies e.g., cloud, advanced data platform.  
* Share coding experience with other team members.  
* The principal function of a Data Engineer is to make specific tasks, based on the business's specifications:  
* Collaborate with Business Analysts (BAs) to design and implement data modeling solutions at Staging, Atomic, Datamart, Sematic layer to provide business intelligence or to provide data to other downstream applications.  
* Design and develop solutions, including comprehensive technical documentation and operational/deployment manuals for the operation of the program by operators.  
* Have deep knowledge in the data domain, with expertise in utilizing and designing/develop solutions based on batch and real-time data processing technologies such as Kafka, Flink, Kafka Connect, Debezium, Spark, Hadoop, EMR, Glue...  
* Have knowledge of AWS cloud and the necessary services/open-source tools for data processing and infrastructure deployment, including EMR, Kubernetes (K8s), EKS, Docker, EventBridge, Lambda,StepFunction, Airflow, Terraform...  
* Have knowledge in optimizing data processing on database systems such as Oracle, SQL Server, and big data systems on the cloud.  
* Updating, repairing, modifying, developing, enhance existing development.  
* Investigate and troubleshoot for issues, incidents, and problems of application as level 3 support.  
* Study the technical updates and make the change or do upgrading release in Development area.  
* Follow the IT processes (development, operation) and Bank processes.

## **Yêu cầu ứng viên**

* A bachelor's degree in computer science, information systems, or equivalent work experience, is required  
* At least 3 working as data developer/engineer with a focus on AWS cloud technologies.  
* Strong hands-on experience with AWS services such as: AWS Glue, Lambda, S3, Redshift, RDS, DynamoDB, Athena, Kinesis, EMR.  
* Proficiency in SQL and experience working with relational and NoSQL databases.  
* Experience with big data processing frameworks such as Apache Spark, Hadoop, or similar.  
* Programming skills in Python, Java, or Scala.  
* Experience with ETL/ELT tools and data pipeline orchestration.  
* Familiarity with Infrastructure as Code (IaC) tools like CloudFormation or Terraform.  
* Familiarity with DevOps practices and CI/CD pipelines  
* 3+ year Experience with Oracle, SQL Server, MySQL, Postgre  
* 3+ year experience in Data Warehouse, Data model, Data Integration, Data mart, design Database  
* An understanding of bank business domain  
* Ability in English reading and writing (mandatory), and speaking, listening (preferable).  
* Teamwork, careful, attention to detail, logical thinking.

