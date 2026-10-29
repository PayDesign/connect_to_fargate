# Fargate接続ツールの説明

## 事前準備
- クライアント側でECS Execを利用可能にしておく
- 参考情報
  - https://docs.aws.amazon.com/ja_jp/AmazonECS/latest/userguide/ecs-exec.html
  - https://docs.aws.amazon.com/ja_jp/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html


## 利用方法

### アクセスキー確認

![アクセスキー確認](./img/aws_access_potal_access_key.png "アクセスキー確認")

### 手動でAWSアクセス資格取得確認

```
$ aws configure --profile "profile_name"
```

`profile_name`は自分のプロフィールをご利用ください。次の説明から`profile_name`は自分のプロファイル名を使用するものとします。


### ブラウザでAWS SSO画面でログインしてAWSアクセス資格取得

```
$ aws configure sso --profile profile_name
```

`profile_name`は自分のプロフィールをご利用ください。

ブラウザでAWS SSOのアクセスキー情報でAWS SSOの接続先の情報をローカルに設定します。

`aws sso login`コマンドでブラウザでログインしてAWSアクセス資格を取得します。この方法を利用すると「手動でAWSアクセス資格取得確認」の方法を利用する必要はありません。

#### 設定情報保存先

`profile_name`は自分のプロフィール名なのか確認ください。

`~/.aws/config`ファイルに認証情報が設定されます。
`~/.aws/config`で設定情報を確認できます。

```
vi ~/.aws/config
```

### Profileを設定して、sso login する

`aws sso login`コマンドを利用してAWSで情報を取得するために、ローカルに設定した認証情報でAWSアクセス資格を取得します。

#### プロフィールを指定してログイン

```
$ export AWS_PROFILE=profile_name; aws sso login
```

`profile_name`は自分のプロフィールをご利用ください。

(url をブラウザで参照し、 codeを入力する)

#### シェルが起動するたびにその変数が自動的にプロフィール指定

bash 環境

```
$ echo 'export AWS_PROFILE=profile_name' >> ~/.bashrc
$ source ~/.bashrc
```

zsh 環境

```
echo 'export AWS_PROFILE=profile_name' >> ~/.zshrc
source ~/.zshrc
```

セール設定ファイルで上の設定が行なってからは`export AWS_PROFILE=profile_name;`を書かなく、`aws sso login`コマンドだけでAWSアクセス資格を取得します。

### AWSアクセス資格取得確認

```
$ aws configure list
```

```
      Name                    Value             Type    Location
      ----                    -----             ----    --------
   profile             profile_name              env    ['AWS_PROFILE', 'AWS_DEFAULT_PROFILE']
access_key     ****************ABCD   shared-credentials-file    
secret_key     ****************EFGH   shared-credentials-file    
    region           ap-northeast-1       config-file    ~/.aws/config
```

### pipコマンド

pythonのインストールが必要

MacOSの場合は`pip`を`pip3`で使用している場合があります。

### モジュールのインストール

```
$ pip install -r requirements.txt
```

### Session Manager プラグインインストール(下記URLはMacOSでのインストール方法)

https://docs.aws.amazon.com/ja_jp/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html#install-plugin-macos

### aws cli のインストール

https://docs.aws.amazon.com/ja_jp/cli/latest/userguide/getting-started-install.html

### リンク作成

```
$ sudo ln -s $(pwd)/connect_to_fargate.py /usr/local/bin
```

初期設定で一回実行する必要があります。一回設定されてからは再度設定する必要はありません。

### ログディレクトリ作成(任意)

```
$ mkdir -p ~/.connect_to_fargate/log
```

初期設定で一回実行する必要があります。一回設定されてからは再度設定する必要はありません。

### スクリプト実行する

```
$ connect_to_fargate.py
```

`-p/--profile` を指定した場合、スクリプト内部で `~/.aws/sso/cache/` の SSO キャッシュを参照します。キャッシュ判定は `botocore` / `boto3` と同じく `sso_session` または `sso_start_url` から求めたキャッシュキーの JSON を利用します。対象プロファイルのキャッシュが存在しない、または `expiresAt` を過ぎている場合は `aws sso logout` -> `aws sso login --profile <profile>` を実行します。

キャッシュ上は有効でも AWS 側で SSO セッションが失効し、`The SSO session associated with this profile has expired or is otherwise invalid.` や `Error loading SSO Token: Token for ... does not exist` が返る場合があります。その場合は `aws sso logout` -> `aws sso login --profile <profile>` を実行してブラウザログイン画面を再表示した上で1回だけ自動再試行します。

`-p/--profile` を省略する場合は、従来通り `AWS_PROFILE` を設定してください。

SSO セッションの有効期限は AWS CLI の SSO キャッシュ `expiresAt` を利用して判定します。

※アプリケーションログは常に `~/.connect_to_fargate/log/` 配下へ出力されます。
実行コマンドが `connect_to_fargate.py` でも `fargatessh` でも、ログディレクトリは共通です。
ファイル名は常に `connect_to_fargate_{cluster_name}_{service_name}_{container_name}_(日時).log` の形式です。

※`aws ecs execute-command` のセッション出力も常に `~/.connect_to_fargate/log/` 配下へ出力されます。
ファイル名は `connect_to_fargate_{cluster_name}_{service_name}_{container_name}_(日時)_session.log` の形式です。

※引数に以下を利用できるように追加しました。

`-p, --profile=profile_name` AWS プロファイル名（未指定時は `AWS_PROFILE` を利用）

--cluster=クラスター名（指定ない場合は、対話型の選択画面に遷移）

--service=サービス名（指定ない場合は、対話型の選択画面に遷移）

--task=タスク名（未指定時は、候補が1件なら自動選択、複数件なら選択ルールに従う）

`--task-selection {prompt,latest,oldest}` 複数タスクの選択方法（既定値: `prompt`）。`--task` とは同時指定不可

`-t, --container=コンテナ名`（指定ない場合は、対話型の選択画面に遷移）

`--container-selection {prompt,auto}` コンテナの選択方法（既定値: `prompt`）。`--container` とは同時指定不可

`--container-selection-fallback {error,prompt}` 自動選択の優先候補が存在しない場合の動作（既定値: `error`）。`--container-selection auto` 指定時のみ利用可能

--cmd=コンテナで実行するコマンド（指定ない場合は、/bin/bash）

`-f, --force` 接続確認なしでログインを行う

`-q, --quiet` 通常メッセージとログファイルのパス通知を画面に表示しない（ファイルへのログ保存は継続）

`--force-login` SSO セッションを強制的に再ログインする（`aws sso logout` -> `aws sso login`）

※Ctrl + Cの挙動は変更しました（接続処理直前にSIGINTをSIG_IGNに変換する処理を追加）

### 実行結果（help）

```
$ connect_to_fargate.py --help
```

```
usage: connect_to_fargate.py [-h] [-p PROFILE] [-c CLUSTER] [-s SERVICE]
                             [--task TASK |
                             --task-selection {prompt,latest,oldest}]
                             [-t CONTAINER | --container-selection {prompt,auto}]
                             [--container-selection-fallback {error,prompt}]
                             [--cmd CMD] [-f] [-q]
                             [--force-login]
```

### タスクの自動選択

`--task` を省略した場合、指定したサービス（または対話画面で選んだスタンドアロンタスク）のうち、`desiredStatus` と `lastStatus` がともに `RUNNING` のタスクを全ページから取得します。

| 候補件数 | 動作 |
| --- | --- |
| 0件 | エラーで終了 |
| 1件 | 選択画面を表示せず自動選択 |
| 複数件 | `--task-selection` のルールに従って1件を選択 |

| ルール | 複数件の場合の動作 |
| --- | --- |
| `prompt`（既定値） | 選択画面を表示 |
| `latest` | 起動日時（`startedAt`）が最も新しいタスクを自動選択 |
| `oldest` | 起動日時（`startedAt`）が最も古いタスクを自動選択 |

起動日時が同じ場合は、タスクIDの昇順で先頭のタスクを選択します。`latest` はタスク定義の最新リビジョンを意味しません。複数件から日時で自動選択する際に起動日時が不明なタスクがあれば、エラーで終了します。その場合は `--task` でタスクIDを指定してください。

```bash
# 1件なら自動選択、複数件なら選択画面
connect_to_fargate.py -p profile -c cluster -s service -t app

# 複数件でも最新のタスクを自動選択し、接続確認を省略してコマンドを実行
connect_to_fargate.py -p profile -c cluster -s service -t app \
  --task-selection latest --cmd 'php artisan --version' -f

# 最も古いタスクに対話ログイン
connect_to_fargate.py -p profile -c cluster -s service -t app \
  --task-selection oldest
```

候補件数、選択ルール、選択したタスクID、起動日時を表示・ログ出力します（`--quiet` 指定時はファイルへの記録のみ）。選択後にタスクが停止した場合はエラーとし、別タスクへの自動切り替えは行いません。

`-f` は接続確認だけを省略します。`--task-selection` はタスク選択にだけ適用されるため、クラスター・サービス・コンテナの未指定時は引き続き選択画面が表示されます。SSO の再ログインが必要な場合はブラウザでの認証も発生します。

### コンテナの自動選択

`--container-selection auto` を指定すると、選択済みタスク内のコンテナ名を完全一致で検索し、`app` → `web` → `scheduler` → `queue` → `email` → `webhook` → `worker` → `docs` → `queue-redis` → `api` → `nginx` → `seven-worker` → `queue-email` の優先順で最初に存在するコンテナを選びます。`App` や `my-app` は `app` と一致しません。

`--task` を指定した場合は、そのタスク内だけを検索します。タスク未指定時は既存のタスク選択ルールで対象を決めてから検索します。スタンドアロンタスクでも同じ動作です。

| 指定 | 動作 |
| --- | --- |
| `--container-selection` 省略、または `prompt` | コンテナ一覧から対話選択（候補が1件でも表示） |
| `auto` で優先候補あり | 優先順で自動選択 |
| `auto` で優先候補なし、フォールバック省略または `error` | 利用可能なコンテナ名をエラーに表示して終了 |
| `auto` で優先候補なし、フォールバックが `prompt` | 同じタスク内のコンテナ一覧から対話選択 |
| コンテナが0件 | フォールバックの指定にかかわらずエラーで終了 |

`--container` と `--container-selection` は同時指定できません。`--container-selection-fallback` は、`error` を明示する場合も `--container-selection auto` の指定が必要です。選択後にコンテナが存在しなくなった場合はエラーとし、別コンテナや別タスクへ自動で切り替えません。

```bash
# 指定タスク内でコンテナを自動選択。優先候補がなければエラー
connect_to_fargate.py -p profile -c cluster -s service --task TASK_ID \
  --container-selection auto

# 優先候補がなければコンテナ一覧から対話選択
connect_to_fargate.py -p profile -c cluster -s service --task TASK_ID \
  --container-selection auto --container-selection-fallback prompt

# タスクとコンテナを自動選択して、接続確認を省略
connect_to_fargate.py -p profile -c cluster -s service \
  --task-selection latest --container-selection auto -f
```

### 通常メッセージの抑制

`-q` または `--quiet` を指定すると、ログファイルのパス・更新通知、SSO の状態、処理開始、プロファイル、タスクの選択結果、接続先の案内、ログイン・ログアウトの案内など、ツール自身の通常メッセージを画面に表示しません。アプリケーションログとセッションログは従来どおり保存します。

```bash
connect_to_fargate.py -p mpfront -c cluster -s service -t app \
  --task-selection latest --cmd 'php artisan --version' -f --quiet
```

警告・エラーは標準エラーに表示します。タスクなどの選択画面と接続確認、コンテナ内コマンドの実行結果、AWS CLI・Session Manager によるセッション開始／終了メッセージ、SSO ログイン時の認証案内は引き続き表示します。`--quiet` 自体は選択や確認を省略しません。


### 実行結果（例１ 引数なし）

```
$ connect_to_fargate.py
```

```
処理を開始します
[?] 接続先が存在するクラスター名を選択してください:
 ❯ default
   cluster_a
   cluster_b

クラスター名: default

[?] 接続先が存在するサービス名を選択してください:
 ❯ service_a
   service_b

サービス名: service_a

[?] 接続先が存在するタスク名を選択してください:
 ❯ 779933414f454c76960d15b37ab15f26
   ae95d0c222424643be7ddd8e288b1229

タスク名: 779933414f454c76960d15b37ab15f26

[?] 接続先のコンテナ名を選択してください: app
 ❯ app
   web

コンテナ名: app

以下のFargateに接続します
----------------------------------------
クラスター名: default
サービス名: service_a
タスク名: 779933414f454c76960d15b37ab15f26
コンテナ名: app
----------------------------------------

[?] こちらに接続してよろしいですか: yes
 ❯ yes
   no

Fargateにログインします

The Session Manager plugin was installed successfully. Use the AWS CLI to start a session.


Starting session with SessionId: ecs-execute-command-08107c57e1eb5fee9
root@ip-172-30-2-204:/#
↑コンテナにログインしている
exit


Exiting session with sessionId: ecs-execute-command-08107c57e1eb5fee9.

CompletedProcess(args='/usr/local/bin/aws ecs execute-command --cluster test-cluster --task 87b5a48c8b99450d9dea5443c863ee5d --container nginx --interactive --command /bin/bash | tee -a ~/.connect_to_fargate/log/connect_to_fargate_test-cluster_test-service_nginx_20220629090944872720_session.log', returncode=0)
Fargateからログアウトしました
```

### 実行結果（例２ 一部引数あり）

```
$ connect_to_fargate.py --cluster=default --container=web
```

```
処理を開始します
[?] 接続先が存在するサービス名を選択してください:
 ❯ service_a
   service_b

サービス名: service_a

[?] 接続先が存在するタスク名を選択してください:
 ❯ 779933414f454c76960d15b37ab15f26
   ae95d0c222424643be7ddd8e288b1229

タスク名: 779933414f454c76960d15b37ab15f26

以下のFargateに接続します
----------------------------------------
クラスター名: default
サービス名: service_a
タスク名: ae95d0c222424643be7ddd8e288b1229
コンテナ名: web
----------------------------------------

[?] こちらに接続してよろしいですか: yes
 ❯ yes
   no

Fargateにログインします
（略）
```

### 実行結果（例３ fargatessh利用）

```
$ fargatessh -p profile -c default -t app -f
```

```
処理を開始します
[?] 接続先が存在するサービス名を選択してください:
 ❯ service_a
   service_b

サービス名: service_a

[?] 接続先が存在するタスク名を選択してください:
 ❯ 779933414f454c76960d15b37ab15f26
   ae95d0c222424643be7ddd8e288b1229

タスク名: 779933414f454c76960d15b37ab15f26

以下のFargateに接続します
----------------------------------------
クラスター名: default
サービス名: service_a
タスク名: ae95d0c222424643be7ddd8e288b1229
コンテナ名: web
----------------------------------------

Fargateにログインします
（略）
```

## トラブルシューティング
'ServiceNotActiveException'が発生した場合
以下を実行して新しいタスクに入れ替える
```
aws ecs update-service \
--cluster (クラスター名) \ 
--service (サービス名) \
--enable-execute-command
'TargetNotConnectedException'が発生した場合
正しいセキュリティグループにしているか、ネットワーク経路が確保されているかを確認する
```

### 汎用的な調査方法
https://github.com/aws-containers/amazon-ecs-exec-checker
check-ecs-execを実行する
```
[実行例]
$ check-ecs-exec.sh (クラスター名) (タスク名)
※/usr/local/binに配置しているため

[実行結果]
-------------------------------------------------------------
Prerequisites for check-ecs-exec.sh v0.7
-------------------------------------------------------------
  jq      | OK (/usr/bin/jq)
  AWS CLI | OK (/usr/local/bin/aws)

-------------------------------------------------------------
Prerequisites for the AWS CLI to use ECS Exec
-------------------------------------------------------------
  AWS CLI Version        | OK (aws-cli/2.6.3 Python/3.9.11 Linux/5.10.118-111.515.amzn2.x86_64 exe/x86_64.amzn.2 prompt/off)
  Session Manager Plugin | OK (1.2.339.0)

-------------------------------------------------------------
Checks on ECS task and other resources
-------------------------------------------------------------
Region : ap-northeast-1
Cluster: (クラスター名)
Task   : (タスク名)
-------------------------------------------------------------
  Cluster Configuration  | Audit Logging Not Configured
  Can I ExecuteCommand?  | arn:aws:iam::172972874842:role/aws-reserved/sso.amazonaws.com/ap-northeast-1/AWSReservedSSO_Administrators@System_10607a14223798a0
     ecs:ExecuteCommand: allowed
     ssm:StartSession denied?: allowed
  Task Status            | RUNNING
  Launch Type            | Fargate
  Platform Version       | 1.4.0
  Exec Enabled for Task  | NO　　　←今回の実行例の場合はこれが原因
  Container-Level Checks |
    ----------
      Managed Agent Status - SKIPPED
    ----------
    ----------
      Init Process Enabled ((タスク名):(タスクのバージョン))
    ----------
         1. Disabled - "web"
         2. Disabled - "app"
    ----------
      Read-Only Root Filesystem ((タスク名):(タスクのバージョン))
    ----------
         1. Disabled - "web"
         2. Disabled - "app"
  Task Role Permissions  | arn:aws:iam::172972874842:role/ecsTaskRole
     ssmmessages:CreateControlChannel: allowed
     ssmmessages:CreateDataChannel: allowed
     ssmmessages:OpenControlChannel: allowed
     ssmmessages:OpenDataChannel: allowed
  VPC Endpoints          |
    Found existing endpoints for vpc-00000000000000:
      - com.amazonaws.ap-northeast-1.s3
      - com.amazonaws.ap-northeast-1.kms
      - com.amazonaws.ap-northeast-1.ec2messages
      - com.amazonaws.ap-northeast-1.ssm
      - com.amazonaws.ap-northeast-1.ssmmessages
      - com.amazonaws.ap-northeast-1.secretsmanager
  Environment Variables  | (wordpress-development:37)
       1. container "web"
       - AWS_ACCESS_KEY: not defined
       - AWS_ACCESS_KEY_ID: not defined
       - AWS_SECRET_ACCESS_KEY: not defined
       2. container "app"
       - AWS_ACCESS_KEY: not defined
       - AWS_ACCESS_KEY_ID: not defined
       - AWS_SECRET_ACCESS_KEY: not defined
 ```
