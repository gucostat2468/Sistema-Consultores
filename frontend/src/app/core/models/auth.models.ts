export type UserRole = 'admin' | 'consultor';

export interface SessionUser {
  id: number;
  name: string;
  username: string;
  role: UserRole;
  email?: string | null;
}

export interface AuthSession {
  accessToken: string;
  user: SessionUser;
}

export interface LoginPayload {
  username: string;
  password: string;
}
